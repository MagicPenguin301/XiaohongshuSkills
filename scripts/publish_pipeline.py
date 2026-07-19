"""
Unified publish pipeline for Xiaohongshu.

Single CLI entry point that orchestrates:
  chrome_launcher → login check → image/video download → form fill → publish (default)

Usage:
    # Publish immediately after filling (default behavior)
    python publish_pipeline.py --title "标题" --content "正文" --image-urls URL1 URL2
    python publish_pipeline.py --title-file t.txt --content-file body.txt --image-urls URL1

    # Fill form only for manual review (preview mode)
    python publish_pipeline.py --title "标题" --content "正文" --image-urls URL1 --preview

    # Headless mode (no GUI window) - faster for automated publishing
    python publish_pipeline.py --headless --title-file t.txt --content-file body.txt --image-urls URL1

    # Publish to a specific account
    python publish_pipeline.py --account myaccount --title "标题" --content "正文" --image-urls URL1

    # Explicit auto-publish flag (optional compatibility flag)
    python publish_pipeline.py --title "标题" --content "正文" --image-urls URL1 --auto-publish

    # Prefer reusing existing tab (reduce focus switching in headed mode)
    python publish_pipeline.py --reuse-existing-tab --title "标题" --content "正文" --image-urls URL1

    # Use local image files instead of URLs
    python publish_pipeline.py --title "标题" --content "正文" --images img1.jpg img2.jpg
    # Skip local file check (for WSL/remote CDP + Windows/UNC paths)
    python publish_pipeline.py --title "标题" --content "正文" --images "\\\\wsl.localhost\\Ubuntu\\home\\me\\a.jpg" --skip-file-check

    # Preserve original Windows/UNC upload paths
    python publish_pipeline.py --title "标题" --content "正文" --images "\\\\wsl.localhost\\Ubuntu\\home\\me\\a.jpg" --skip-file-check --preserve-upload-paths

    # Publish a video (local file)
    python publish_pipeline.py --title "标题" --content "正文" --video video.mp4

    # Publish a video (from URL)
    python publish_pipeline.py --title "标题" --content "正文" --video-url "https://example.com/video.mp4"

Exit codes:
    0 = success (PUBLISHED, or READY_TO_PUBLISH in preview mode)
    1 = not logged in (NOT_LOGGED_IN) - headless auto-fallback will restart headed
    2 = error (see stderr)
"""

import argparse
import json
import os
import random
import re
import sys
import time
from urllib.parse import unquote, urlparse, urlunparse

# Ensure UTF-8 output on Windows consoles
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Add scripts dir to path so sibling modules can be imported
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from chrome_launcher import ensure_chrome, restart_chrome
from cdp_publish import XiaohongshuPublisher, CDPError
from image_downloader import ImageDownloader
from run_lock import SingleInstanceError, single_instance


MAX_TIMING_JITTER_RATIO = 0.7


def _normalize_timing_jitter(value: float) -> float:
    """Clamp timing jitter to a safe range."""
    return max(0.0, min(MAX_TIMING_JITTER_RATIO, value))


def _is_local_host(host: str) -> bool:
    """Return True when host points to the local machine."""
    return host.strip().lower() in {"127.0.0.1", "localhost", "::1"}


def _split_proxy_auth(proxy_server: str | None) -> dict[str, str | None]:
    """Return Chrome-safe proxy server plus optional auth from a proxy URL."""
    proxy = str(proxy_server or "").strip()
    if not proxy:
        return {"server": None, "username": None, "password": None}

    parsed = urlparse(proxy)
    if not parsed.scheme or not parsed.netloc or parsed.username is None:
        return {"server": proxy, "username": None, "password": None}

    host = parsed.hostname or ""
    if not host:
        return {"server": proxy, "username": None, "password": None}
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    netloc = f"{host}:{parsed.port}" if parsed.port is not None else host
    server = urlunparse((parsed.scheme, netloc, "", "", "", ""))
    return {
        "server": server,
        "username": unquote(parsed.username),
        "password": unquote(parsed.password or ""),
    }


def _resolve_account_name(account_name: str | None) -> str:
    """Resolve explicit or default account name for login cache scoping."""
    if account_name and account_name.strip():
        return account_name.strip()
    try:
        from account_manager import get_default_account
        resolved = get_default_account()
        if isinstance(resolved, str) and resolved.strip():
            return resolved.strip()
    except Exception:
        pass
    return "default"


def _jitter_ms(base_ms: int, jitter_ratio: float, minimum_ms: int = 0) -> int:
    """Return a randomized delay in milliseconds around the base value."""
    base = max(minimum_ms, int(base_ms))
    if jitter_ratio <= 0:
        return base

    delta = int(round(base * jitter_ratio))
    low = max(minimum_ms, base - delta)
    high = max(low, base + delta)
    return random.randint(low, high)


def _jitter_seconds(
    base_seconds: float,
    jitter_ratio: float,
    minimum_seconds: float = 0.05,
) -> float:
    """Return a randomized delay in seconds around the base value."""
    base = max(minimum_seconds, float(base_seconds))
    if jitter_ratio <= 0:
        return base

    delta = base * jitter_ratio
    low = max(minimum_seconds, base - delta)
    high = max(low, base + delta)
    return random.uniform(low, high)


def _extract_topic_tags_from_last_line(content: str) -> tuple[str, list[str]]:
    """Extract topic tags from the last non-empty line.

    Expected format of the last line: "#标签1 #标签2 #标签3"
    Returns:
        (content_without_tag_line, tags)
    """
    lines = content.splitlines()

    # Ignore trailing blank lines when finding the last meaningful line.
    while lines and not lines[-1].strip():
        lines.pop()

    if not lines:
        return content, []

    last_line = lines[-1].strip()
    parts = [p for p in last_line.split() if p]
    if not parts:
        return content, []

    # Every token must look like '#xxx' and cannot contain spaces.
    if not all(re.fullmatch(r"#[^\s#]+", part) for part in parts):
        return content, []

    body = "\n".join(lines[:-1]).strip()
    return body, parts


def _verify_local_files_exist(
    file_paths: list[str],
    media_label: str,
    skip_file_check: bool,
):
    """Verify local files exist unless explicitly skipped."""
    if skip_file_check:
        print(
            f"[pipeline] Step 3: Skipping local {media_label} file check "
            "(--skip-file-check)."
        )
        return

    for file_path in file_paths:
        if not os.path.isfile(file_path):
            print(f"Error: {media_label} file not found: {file_path}", file=sys.stderr)
            sys.exit(2)


def _select_topics(
    publisher: XiaohongshuPublisher,
    tags: list[str],
    timing_jitter: float = 0.25,
):
    """Type each tag, wait for suggestions, then confirm with Enter."""
    if not tags:
        return

    if hasattr(publisher, "_wait_for_content_editor_ready"):
        publisher._wait_for_content_editor_ready(timeout_seconds=120.0)

    print(f"[pipeline] Step 4.1: Selecting {len(tags)} topic tag(s)...")
    failed_tags = []

    for index, tag in enumerate(tags):
        normalized_tag = tag.lstrip("#").strip()
        if not normalized_tag:
            continue

        hash_pause_ms = _jitter_ms(180, timing_jitter, minimum_ms=90)
        char_delay_min_ms = _jitter_ms(45, timing_jitter, minimum_ms=25)
        char_delay_max_ms = _jitter_ms(95, timing_jitter, minimum_ms=char_delay_min_ms)
        suggest_wait_ms = _jitter_ms(5000, timing_jitter, minimum_ms=2600)
        after_enter_ms = _jitter_ms(900, timing_jitter, minimum_ms=500)

        escaped_tag = json.dumps(normalized_tag)
        newline_literal = json.dumps("\n")
        hash_literal = json.dumps("#")
        space_literal = json.dumps(" ")
        result = publisher._evaluate(f"""
            (async function() {{
                var editor = document.querySelector(
                    'div.tiptap.ProseMirror, div.ProseMirror[contenteditable="true"]'
                );
                if (!editor) {{
                    return {{ ok: false, reason: 'editor_not_found' }};
                }}

                function sleep(ms) {{
                    return new Promise(function(resolve) {{ setTimeout(resolve, ms); }});
                }}

                function moveCaretToEditorEnd(el) {{
                    el.focus();
                    var selection = window.getSelection();
                    if (!selection) return false;
                    var range = document.createRange();
                    range.selectNodeContents(el);
                    range.collapse(false);
                    selection.removeAllRanges();
                    selection.addRange(range);
                    return true;
                }}

                function insertTextAtCaret(text) {{
                    moveCaretToEditorEnd(editor);
                    var inserted = false;
                    try {{
                        inserted = document.execCommand('insertText', false, text);
                    }} catch (e) {{}}

                    if (!inserted) {{
                        var selection = window.getSelection();
                        if (selection && selection.rangeCount > 0) {{
                            var range = selection.getRangeAt(0);
                            var node = document.createTextNode(text);
                            range.insertNode(node);
                            range.setStartAfter(node);
                            range.collapse(true);
                            selection.removeAllRanges();
                            selection.addRange(range);
                        }} else {{
                            editor.appendChild(document.createTextNode(text));
                        }}
                    }}
                    editor.dispatchEvent(new Event('input', {{ bubbles: true }}));
                }}

                function countTopicTokens() {{
                    var nodes = editor.querySelectorAll(
                        '[contenteditable="false"], [data-type*="topic"], [class*="topic"], [class*="tag"], a'
                    );
                    var count = 0;
                    for (var i = 0; i < nodes.length; i++) {{
                        var text = String(nodes[i].innerText || nodes[i].textContent || '').trim();
                        var className = String(nodes[i].className || '');
                        var marker = String(nodes[i].getAttribute('data-type') || '');
                        if (
                            text.indexOf('#') >= 0 ||
                            /topic|tag/i.test(className) ||
                            /topic|tag/i.test(marker)
                        ) {{
                            count += 1;
                        }}
                    }}
                    return count;
                }}

                function pressEnter(el) {{
                    var evt = {{
                        key: 'Enter',
                        code: 'Enter',
                        keyCode: 13,
                        which: 13,
                        bubbles: true,
                        cancelable: true,
                    }};
                    el.dispatchEvent(new KeyboardEvent('keydown', evt));
                    el.dispatchEvent(new KeyboardEvent('keypress', evt));
                    el.dispatchEvent(new KeyboardEvent('keyup', evt));
                }}

                var beforeTopicCount = countTopicTokens();
                moveCaretToEditorEnd(editor);
                if ({index} === 0) {{
                    insertTextAtCaret({newline_literal});
                }}
                insertTextAtCaret({hash_literal});
                await sleep({hash_pause_ms});

                var tagText = {escaped_tag};
                var charDelayMin = {char_delay_min_ms};
                var charDelayMax = {char_delay_max_ms};
                for (var i = 0; i < tagText.length; i++) {{
                    moveCaretToEditorEnd(editor);
                    insertTextAtCaret(tagText[i]);
                    var charDelay = Math.floor(Math.random() * (charDelayMax - charDelayMin + 1)) + charDelayMin;
                    await sleep(charDelay);
                }}

                await sleep({suggest_wait_ms});
                moveCaretToEditorEnd(editor);
                pressEnter(editor);
                await sleep({after_enter_ms});
                var afterTopicCount = countTopicTokens();
                moveCaretToEditorEnd(editor);
                insertTextAtCaret({space_literal});
                return {{
                    ok: true,
                    selected: true,
                    topicCountBefore: beforeTopicCount,
                    topicCountAfter: afterTopicCount,
                }};
            }})()
        """)

        if not (isinstance(result, dict) and result.get("ok")):
            failed_tags.append(tag)
            reason = result.get("reason") if isinstance(result, dict) else "unknown"
            print(f"[pipeline] Warning: Failed to select topic {tag} ({reason}).")
        else:
            print(f"[pipeline] Topic selected: {tag}")

        if index < len(tags) - 1:
            time.sleep(_jitter_seconds(0.45, timing_jitter, minimum_seconds=0.2))

    if failed_tags:
        print(
            "[pipeline] Warning: Some topic tags were not selected: "
            f"{', '.join(failed_tags)}"
        )


def _select_collection(
    publisher: XiaohongshuPublisher,
    collection_name: str | None,
    timing_jitter: float = 0.25,
):
    """Open the collection menu and select the named collection."""
    collection_name = str(collection_name or "").strip()
    if not collection_name:
        return

    if hasattr(publisher, "_wait_for_content_editor_ready"):
        publisher._wait_for_content_editor_ready(timeout_seconds=120.0)

    print(f"[pipeline] Step 4.2: Selecting collection: {collection_name}")
    button_result = publisher._evaluate("""
        (function() {
            function visible(el) {
                if (!el) return false;
                var rect = el.getBoundingClientRect();
                var style = window.getComputedStyle(el);
                return rect.width > 0 && rect.height > 0
                    && style.visibility !== 'hidden'
                    && style.display !== 'none';
            }
            var selectors = [
                '.collection-plugin-button',
                '[class*="collection-plugin-button"]',
                'button',
                '[role="button"]'
            ];
            var candidates = [];
            selectors.forEach(function(selector) {
                document.querySelectorAll(selector).forEach(function(el) {
                    if (candidates.indexOf(el) === -1) candidates.push(el);
                });
            });
            for (var i = 0; i < candidates.length; i++) {
                var el = candidates[i];
                var text = String(el.innerText || el.textContent || '').trim();
                var className = String(el.className || '');
                if (visible(el) && (className.indexOf('collection-plugin-button') >= 0 || text.indexOf('选择合集') >= 0)) {
                    var rect = el.getBoundingClientRect();
                    return { ok: true, rect: { x: rect.x, y: rect.y, width: rect.width, height: rect.height } };
                }
            }
            return { ok: false, reason: 'button_not_found' };
        })()
    """)
    if not (isinstance(button_result, dict) and button_result.get("ok")):
        reason = button_result.get("reason") if isinstance(button_result, dict) else "unknown"
        print(f"[pipeline] Warning: Could not find collection button ({reason}).")
        return

    _click_rect(publisher, button_result["rect"])
    time.sleep(_jitter_seconds(0.8, timing_jitter, minimum_seconds=0.4))

    target_literal = json.dumps(collection_name)
    item_result = publisher._evaluate(f"""
        (async function() {{
            var target = {target_literal};
            function norm(value) {{
                return String(value || '').replace(/\\s+/g, '').trim();
            }}
            function delay(ms) {{
                return new Promise(function(resolve) {{ setTimeout(resolve, ms); }});
            }}
            function visible(el) {{
                if (!el) return false;
                var rect = el.getBoundingClientRect();
                var style = window.getComputedStyle(el);
                return rect.width > 0 && rect.height > 0
                    && style.visibility !== 'hidden'
                    && style.display !== 'none';
            }}
            var options = [];
            function roots() {{
                var found = Array.prototype.slice.call(document.querySelectorAll(
                    '.collection-plugin-popover, [class*="collection-plugin-popover"], .d-popover'
                )).filter(visible);
                return found.length ? found : [document.body];
            }}
            function rememberOption(text) {{
                if (text && options.indexOf(text) === -1) options.push(text);
            }}
            function optionText(node) {{
                return String(node.innerText || node.textContent || '').replace('创建合集', '').trim();
            }}
            function candidateNodes(root) {{
                return Array.prototype.slice.call(root.querySelectorAll(
                    '.item-label, [class*="item-label"], .item, [class*="item"]'
                ));
            }}
            function clickElement(el) {{
                var rect = el.getBoundingClientRect();
                var init = {{
                    bubbles: true,
                    cancelable: true,
                    view: window,
                    clientX: rect.left + rect.width / 2,
                    clientY: rect.top + rect.height / 2
                }};
                el.dispatchEvent(new MouseEvent('mousedown', init));
                el.dispatchEvent(new MouseEvent('mouseup', init));
                el.dispatchEvent(new MouseEvent('click', init));
                if (typeof el.click === 'function') el.click();
                return {{ x: rect.x, y: rect.y, width: rect.width, height: rect.height }};
            }}
            function findTarget() {{
                var currentRoots = roots();
                for (var r = 0; r < currentRoots.length; r++) {{
                    var nodes = candidateNodes(currentRoots[r]);
                    for (var i = 0; i < nodes.length; i++) {{
                        var node = nodes[i];
                        var text = optionText(node);
                        if (!text) continue;
                        if (visible(node)) rememberOption(text);
                        if (norm(text) === norm(target)) {{
                            var clickable = node.closest('.item, [class*="item"]') || node;
                            clickable.scrollIntoView({{ block: 'center', inline: 'nearest' }});
                            return clickable;
                        }}
                    }}
                }}
                return null;
            }}
            function scrollContainers() {{
                var containers = [];
                roots().forEach(function(root) {{
                    containers.push(root);
                    root.querySelectorAll('*').forEach(function(el) {{
                        if (el.scrollHeight > el.clientHeight + 4) containers.push(el);
                    }});
                }});
                return containers.filter(function(el, index) {{
                    return containers.indexOf(el) === index && visible(el);
                }});
            }}

            var direct = findTarget();
            if (direct) {{
                await delay(80);
                return {{ ok: true, clicked: true, rect: clickElement(direct), options: options }};
            }}

            var containers = scrollContainers();
            for (var c = 0; c < containers.length; c++) {{
                var el = containers[c];
                var maxScroll = Math.max(0, el.scrollHeight - el.clientHeight);
                if (!maxScroll) continue;
                var originalTop = el.scrollTop;
                var steps = 8;
                for (var step = 0; step <= steps; step++) {{
                    el.scrollTop = Math.round(maxScroll * step / steps);
                    await delay(120);
                    var targetEl = findTarget();
                    if (targetEl) {{
                        await delay(80);
                        return {{ ok: true, clicked: true, rect: clickElement(targetEl), options: options }};
                    }}
                }}
                el.scrollTop = originalTop;
            }}
            return {{ ok: false, reason: 'collection_not_found', options: options }};
        }})()
    """)
    if not (isinstance(item_result, dict) and item_result.get("ok")):
        options = item_result.get("options") if isinstance(item_result, dict) else []
        suffix = f" Available: {', '.join(options)}" if options else ""
        print(f"[pipeline] Warning: Could not find collection '{collection_name}'.{suffix}")
        return

    if not item_result.get("clicked"):
        _click_rect(publisher, item_result["rect"])
    time.sleep(_jitter_seconds(0.5, timing_jitter, minimum_seconds=0.25))
    print(f"[pipeline] Collection selected: {collection_name}")


def _click_rect(publisher: XiaohongshuPublisher, rect: dict):
    cx = rect["x"] + rect["width"] / 2
    cy = rect["y"] + rect["height"] / 2
    if hasattr(publisher, "_click_mouse"):
        publisher._click_mouse(cx, cy)
        return
    for event_type in ("mousePressed", "mouseReleased"):
        publisher._send("Input.dispatchMouseEvent", {
            "type": event_type,
            "x": cx,
            "y": cy,
            "button": "left",
            "clickCount": 1,
        })
        time.sleep(0.05)


def main():
    parser = argparse.ArgumentParser(
        description="Xiaohongshu publish pipeline - unified entry point"
    )

    # Title
    title_group = parser.add_mutually_exclusive_group(required=True)
    title_group.add_argument("--title", help="Article title text")
    title_group.add_argument("--title-file", help="Read title from UTF-8 file")

    # Content
    content_group = parser.add_mutually_exclusive_group(required=True)
    content_group.add_argument("--content", help="Article body text")
    content_group.add_argument("--content-file", help="Read content from UTF-8 file")

    # Scheduled publishing
    parser.add_argument(
        "--post-time",
        default=None,
        help="Timer for publishing on note",
    )
    parser.add_argument(
        "--collection",
        default=None,
        help="Collection name to select after filling the note, e.g. Agent篇",
    )

    # Media: images OR video (mutually exclusive)
    media_group = parser.add_mutually_exclusive_group(required=True)
    media_group.add_argument(
        "--image-urls", nargs="+", help="Image URLs to download"
    )
    media_group.add_argument(
        "--images", nargs="+", help="Local image file paths"
    )
    media_group.add_argument(
        "--video", help="Local video file path"
    )
    media_group.add_argument(
        "--video-url", help="Video URL to download"
    )

    # Publish mode
    parser.add_argument(
        "--auto-publish",
        action="store_true",
        default=False,
        help=(
            "Compatibility flag. Publish is now the default behavior unless "
            "--preview is enabled."
        ),
    )

    parser.add_argument(
        "--preview",
        action="store_true",
        default=False,
        help="Preview mode: fill content only and never click publish button",
    )

    # Headless mode
    parser.add_argument(
        "--headless",
        action="store_true",
        default=False,
        help="Run Chrome in headless mode (no GUI). Auto-falls back to headed if login is needed.",
    )

    parser.add_argument(
        "--timing-jitter",
        type=float,
        default=0.25,
        help=(
            "Timing jitter ratio for operation delays (default: 0.25). "
            "Set 0 to disable random jitter."
        ),
    )

    parser.add_argument(
        "--reuse-existing-tab",
        action="store_true",
        default=False,
        help=(
            "Prefer reusing an existing Chrome tab before creating a new one. "
            "Useful in headed mode to reduce foreground focus switching."
        ),
    )
    parser.add_argument(
        "--proxy-server",
        default=None,
        help=(
            "Chrome proxy server for local browser launch, "
            "e.g. http://127.0.0.1:7890. Ignored in remote CDP mode."
        ),
    )
    parser.add_argument("--proxy-username", default=None, help="Proxy authentication username")
    parser.add_argument("--proxy-password", default=None, help="Proxy authentication password")

    # Optional temp dir for downloaded images
    parser.add_argument(
        "--temp-dir",
        default=None,
        help="Directory for downloaded images (default: auto-created temp dir)",
    )
    parser.add_argument(
        "--skip-file-check",
        action="store_true",
        default=False,
        help=(
            "Skip local media file existence check. Useful when running in WSL "
            "or using remote CDP with Windows/UNC paths."
        ),
    )
    parser.add_argument(
        "--preserve-upload-paths",
        action="store_true",
        default=False,
        help=(
            "Force preserving original upload file paths instead of converting "
            "backslashes to forward slashes before DOM.setFileInputFiles. "
            "Windows/UNC paths are auto-detected by default."
        ),
    )

    # Account selection
    parser.add_argument(
        "--account",
        default=None,
        help="Account name to publish to (default: default account)",
    )

    # CDP port
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="CDP host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9222,
        help="CDP remote debugging port (default: 9222)",
    )

    args = parser.parse_args()
    host = args.host
    port = args.port
    headless = args.headless
    account = args.account
    cache_account_name = _resolve_account_name(account)
    reuse_existing_tab = args.reuse_existing_tab
    proxy_parts = _split_proxy_auth(args.proxy_server)
    proxy_server = proxy_parts["server"]
    proxy_username = args.proxy_username or proxy_parts["username"]
    proxy_password = args.proxy_password or proxy_parts["password"]
    timing_jitter = _normalize_timing_jitter(args.timing_jitter)
    local_mode = _is_local_host(host)
    post_time = args.post_time

    if timing_jitter != args.timing_jitter:
        print(
            "[pipeline] Warning: --timing-jitter out of range. "
            f"Clamped to {timing_jitter:.2f}."
        )

    # --- Resolve title ---
    if args.title_file:
        with open(args.title_file, encoding="utf-8") as f:
            title = f.read().strip()
    else:
        title = args.title

    if not title:
        print("Error: title is empty.", file=sys.stderr)
        sys.exit(2)

    # --- Resolve content ---
    if args.content_file:
        with open(args.content_file, encoding="utf-8") as f:
            content = f.read().strip()
    else:
        content = args.content

    if not content:
        print("Error: content is empty.", file=sys.stderr)
        sys.exit(2)

    content, topic_tags = _extract_topic_tags_from_last_line(content)
    if topic_tags:
        print(
            "[pipeline] Detected topic tags from last line: "
            f"{' '.join(topic_tags)}"
        )

    # --- Step 1: Ensure Chrome is running ---
    mode_label = "headless" if headless else "headed"
    account_label = cache_account_name
    print(
        f"[pipeline] Step 1: Ensuring Chrome is running "
        f"({mode_label}, account: {account_label}, host: {host}, port: {port})..."
    )
    print(f"[pipeline] Timing jitter ratio: {timing_jitter:.2f}")
    if reuse_existing_tab:
        print("[pipeline] Tab selection mode: prefer reusing existing tab.")
    if proxy_server:
        proxy_note = "local Chrome launch" if local_mode else "ignored in remote CDP mode"
        print(f"[pipeline] Chrome proxy server: {proxy_server} ({proxy_note}).")
    if proxy_username or proxy_password:
        print("[pipeline] Proxy authentication: enabled.")
    if local_mode:
        if not ensure_chrome(
            port=port,
            headless=headless,
            account=account,
            proxy_server=proxy_server,
        ):
            print("Error: Failed to start Chrome.", file=sys.stderr)
            sys.exit(2)
    else:
        print(
            f"[pipeline] Remote CDP mode enabled: {host}:{port}. "
            "Skipping local Chrome launch/restart."
        )

    # --- Step 2: Connect and check login ---
    print("[pipeline] Step 2: Checking login status...")
    publisher = XiaohongshuPublisher(
        host=host,
        port=port,
        timing_jitter=timing_jitter,
        account_name=cache_account_name,
        preserve_upload_paths=args.preserve_upload_paths,
        proxy_server=proxy_server,
        proxy_username=proxy_username,
        proxy_password=proxy_password,
    )
    try:
        publisher.connect(reuse_existing_tab=reuse_existing_tab)
        logged_in = publisher.check_login()
        if not logged_in:
            publisher.disconnect()
            if headless:
                if local_mode:
                    # Auto-fallback: restart Chrome in headed mode for QR login
                    print("[pipeline] Headless mode: not logged in. Switching to headed mode for login...")
                    restart_chrome(
                        port=port,
                        headless=False,
                        account=account,
                        proxy_server=proxy_server,
                    )
                    publisher.connect(reuse_existing_tab=reuse_existing_tab)
                    publisher.open_login_page()
                else:
                    print(
                        "[pipeline] Headless + remote mode: cannot auto-restart remote Chrome. "
                        "Attempting to open login page on existing remote browser..."
                    )
                    publisher.connect(reuse_existing_tab=reuse_existing_tab)
                    publisher.open_login_page()
            print("NOT_LOGGED_IN")
            sys.exit(1)
    except CDPError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)

    # --- Determine publish mode: video or image ---
    is_video_mode = bool(args.video or args.video_url)

    # --- Step 3: Prepare media ---
    image_paths = []
    video_path = None
    downloader = None

    if is_video_mode:
        if args.video_url:
            print("[pipeline] Step 3: Downloading video...")
            downloader = ImageDownloader(temp_dir=args.temp_dir)
            video_path = downloader.download_video(args.video_url)
            if not video_path:
                print("Error: Video download failed.", file=sys.stderr)
                sys.exit(2)
        else:
            video_path = args.video
            _verify_local_files_exist(
                file_paths=[video_path],
                media_label="Video",
                skip_file_check=args.skip_file_check,
            )
            print(f"[pipeline] Step 3: Using local video: {video_path}")
    elif args.image_urls:
        print(f"[pipeline] Step 3: Downloading {len(args.image_urls)} image(s)...")
        downloader = ImageDownloader(temp_dir=args.temp_dir)
        image_paths = downloader.download_all(args.image_urls)
        if not image_paths:
            print("Error: All image downloads failed.", file=sys.stderr)
            sys.exit(2)
    else:
        image_paths = args.images
        _verify_local_files_exist(
            file_paths=image_paths,
            media_label="Image",
            skip_file_check=args.skip_file_check,
        )
        print(f"[pipeline] Step 3: Using {len(image_paths)} local image(s).")

    # --- Step 4: Fill form ---
    print("[pipeline] Step 4: Filling form...")
    try:
        if is_video_mode:
            publisher.publish_video(
                title=title, content=content, video_path=video_path
            )
        else:
            publisher.publish(
                title=title, content=content, image_paths=image_paths, post_time=post_time
            )
        _select_topics(publisher, topic_tags, timing_jitter=timing_jitter)
        _select_collection(publisher, args.collection, timing_jitter=timing_jitter)
        print("FILL_STATUS: READY_TO_PUBLISH")
    except CDPError as e:
        print(f"Error during form fill: {e}", file=sys.stderr)
        if downloader:
            downloader.cleanup()
        sys.exit(2)

    # --- Step 5: Publish (optional) ---
    should_publish = not args.preview
    if args.auto_publish:
        print("[pipeline] --auto-publish is now default and can be omitted.")
    if args.preview:
        print("[pipeline] Preview mode is on, skipping publish click.")

    if should_publish:
        print("[pipeline] Step 5: Clicking publish button...")
        try:
            note_link = publisher._click_publish(post_time != None)
            print("PUBLISH_STATUS: PUBLISHED")
            if note_link:
                print(f"[pipeline] Note published at: {note_link}")
        except CDPError as e:
            print(f"Error clicking publish: {e}", file=sys.stderr)
            if downloader:
                downloader.cleanup()
            sys.exit(2)

    # --- Cleanup ---
    publisher.disconnect()
    if downloader:
        downloader.cleanup()

    print("[pipeline] Done.")


if __name__ == "__main__":
    try:
        with single_instance("post_to_xhs_publish"):
            main()
    except SingleInstanceError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(3)
