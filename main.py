import argparse
import re
import asyncio
import traceback
import random
from urllib.parse import urlparse
from pyppeteer import launch


async def crawl_pages(
    urls: set[str],
    output_dir: str,
    target_domains: set = set(),
    processed_urls: set = set(),
    excluded_urls: set = set(),
    redirected_urls: dict = {},
    depth: int = -1,
    limit: int = 5,  # 同時にクロールするURLは5つまで
):
    if not bool(urls):  # クロール対象URLリスト(urls)が空の場合は処理を中断
        return

    sem = asyncio.Semaphore(limit)

    async def crawl_page(url: str, output_dir: str, sub_urls: set = set()):
        if url in excluded_urls:  # excluded_urls に含まれるURLはクロール対象から除外
            return

        async def redirect_handler(response):
            status = response.status
            if 300 <= status <= 399:
                redirected_urls[response.url] = response.headers["location"]
                print(
                    f"[Detected redirect]{response.url} -> {response.headers['location']}"
                )

        async with sem:
            try:
                browser = await launch(headless=True)
                page = await browser.newPage()
                page.on(
                    "response",
                    lambda response: asyncio.ensure_future(redirect_handler(response)),
                )
                await page.goto(url, waitUntil="networkidle0")  # ページ読み込みが完了するまで待機

                # ページ内のコンテンツを取得
                content = await page.content()

                # ページ内のリンクリストを取得
                sub_links = await page.evaluate(
                    """() => {
                    return Array.from(document.querySelectorAll('a'))
                        .map(link => link.href);
                }"""
                )
                for sub_link in sub_links:
                    sub_urls.add(sub_link)

                await page.close()
                await browser.close()

                # クローリングしたデータを出力
                output_filename = re.sub(r"[^\-0-9a-zA-Z\.]", "_", url)
                print(output_filename)
                output_filepath = "/".join([output_dir, output_filename])
                with open(output_filepath, "w") as fout:
                    fout.write(content)

                # ページ内のリンクリストを出力
                output_filename = re.sub(r"[^\-0-9a-zA-Z\.]", "_", url)
                output_filename = f"urls_in_{output_filename}"
                print(output_filename)
                output_filepath = "/".join([output_dir, output_filename])
                with open(output_filepath, "w") as fout:
                    for sub_link in sub_links:
                        sub_link = sub_link.strip()
                        if not sub_link:  # 空文字は出力しない
                            continue
                        fout.write(f"{sub_link}\n")
                processed_urls.add(url)  # クロール済みURLは、処理済みURLリストに追加
            except Exception:
                traceback.print_exc()

    sub_urls = set()
    randomized_urls = list(urls)
    random.shuffle(randomized_urls)
    tasks = [crawl_page(url, output_dir, sub_urls) for url in randomized_urls]
    await asyncio.gather(*tasks)

    # クロール済みリンク、外部リンクは、次のクロール対象(next_urls)から除去する
    next_urls = set()
    links = (urls | sub_urls) - processed_urls - excluded_urls
    for link in links:
        link_parsed_url = urlparse(link)
        link_domain = link_parsed_url.hostname
        if link_domain in target_domains:  # クロール対象ドメイン配下のリンクのみnext_urlsに追加
            next_urls.add(link)
        else:  # 外部リンクは excluded_urlsに追加
            excluded_urls.add(link)

    if depth > 0 or depth < 0:  # 探索の深さが 0 以外の場合に、次の深さのクロールを実行
        await crawl_pages(
            next_urls,
            output_dir,
            target_domains,
            processed_urls,
            excluded_urls,
            redirected_urls,
            depth - 1,
            limit,
        )


async def main(args):
    global browser
    output_dir = "./output"

    urls = set()
    target_domains = set()
    with open(args.urllistfile, "r") as f:
        while True:
            line = f.readline()
            if not line:
                break
            url = line.strip()
            if url.startswith("#"):
                continue
            parsed_url = urlparse(url)
            target_domain = parsed_url.hostname
            urls.add(url)
            target_domains.add(target_domain)

    processed_urls = set()
    excluded_urls = set()
    redirected_urls = {}
    await crawl_pages(
        urls,
        output_dir,
        target_domains,
        processed_urls,
        excluded_urls,
        redirected_urls,
        depth=args.depth,
        limit=args.limit,
    )

    # 処理済みURLのリストを出力
    output_filename = "all_processed_urls.txt"
    print(output_filename)
    output_filepath = "/".join([output_dir, output_filename])
    with open(output_filepath, "w") as fout:
        for url in processed_urls:
            url = url.strip()
            if not url:  # 空文字は出力しない
                continue
            fout.write(f"{url}\n")

    # 処理対象外URLのリストを出力
    output_filename = "all_excluded_urls.txt"
    print(output_filename)
    output_filepath = "/".join([output_dir, output_filename])
    with open(output_filepath, "w") as fout:
        for url in excluded_urls:
            url = url.strip()
            if not url:  # 空文字は出力しない
                continue
            fout.write(f"{url}\n")

    # リダイレクトURLのリストを出力
    output_filename = "all_redirected_urls.txt"
    print(output_filename)
    output_filepath = "/".join([output_dir, output_filename])
    with open(output_filepath, "w") as fout:
        for url, location in redirected_urls.items():
            url = url.strip()
            if not url:  # 空文字は出力しない
                continue
            location = location.strip()
            fout.write(f"{url} => {location}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A sample crawler using Puppeteer")
    parser.add_argument("urllistfile", type=str, help="URL list for crawling")
    parser.add_argument(
        "--depth", type=int, default=-1, help="Set the maximum number of sublinks"
    )
    parser.add_argument(
        "--limit", type=int, default=-1, help="Set the concurrent crawl executions"
    )
    args = parser.parse_args()
    asyncio.get_event_loop().run_until_complete(main(args))
