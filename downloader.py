import asyncio
import aiohttp
from pathlib import Path
from zipfile import ZipFile
from tqdm.asyncio import tqdm_asyncio
from typing import List

async def retry(coro, n_attempts, delay, timeout, *args, **kwargs):
    for attempt in range(1, n_attempts + 1):
        try:
            return await asyncio.wait_for(
                coro(*args, **kwargs),
                timeout=timeout
            )
        except Exception as e:
            if attempt == n_attempts:
                raise
            await asyncio.sleep(delay)

async def download_zip(
        session: aiohttp.ClientSession,
        url: str,
        target_dir: str,
        use_tqdm : bool = True
        ):
    
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    zip_path = target_dir / "dataset.zip"

    async with session.get(url) as resp:
        resp.raise_for_status()
        total = resp.content_length or 0

        if use_tqdm:
            bar = tqdm_asyncio(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"Downloading {url}",
                mininterval=10
            )
        else:
            bar = None

        with open(zip_path, "wb") as f:
            async for chunk in resp.content.iter_chunked(8192):
                f.write(chunk)
                if use_tqdm:
                    bar.update(len(chunk))

        if use_tqdm:
            bar.close()

    with ZipFile(zip_path, "r") as z:
        bad = z.testzip()
        if bad:
            raise ValueError(f"Corrupted ZIP file")
        
    with ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(target_dir)

    zip_path.unlink()

    if not use_tqdm:
        print(f"Downloaded and extracted to {target_dir}")

    return 1

async def download_and_extract_all(
    urls: List[str],
    paths: List[str],
    use_tqdm: bool = False,
    concurrency_limit: int = 3,
    timeout: int = 600,
    hard_timeout = None,
    custom_header = None,
    retries : bool = False
):
    paths = [Path(p) for p in paths]
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)

    headers = {
        'UserAgent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36 Edg/154.0.0.0',
        'Accept' : '*/*',
        'Accept-Encoding' : 'gzip, deflate, br, zstd',
        'Accept-Language' : 'de,de-DE;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,en-DE;q=0.5',
        'Cache-Control' : 'no-cache',
        'Connection' : 'keep-alive',
        'Max-Forwards' : '1',
        'Accept-Charset' : 'utf-8, iso-8859-1'
    } if custom_header is None else custom_header

    sem = asyncio.Semaphore(concurrency_limit) if isinstance(concurrency_limit, int) else None
    timeout = aiohttp.ClientTimeout(total=timeout)
    connector = aiohttp.TCPConnector(limit=0) #unlimited connection

    async with aiohttp.ClientSession(timeout=timeout,headers=headers,connector=connector) as session:
        tasks = []
        for i, (url, path) in enumerate(zip(urls, paths)):
            dataset_dir = path

            if retries:
                tasks.append(
                    retry(download_zip, 3, 120, hard_timeout, session, url, dataset_dir, use_tqdm=use_tqdm)
                )
            else:
                tasks.append(
                    download_zip(session, url, dataset_dir, use_tqdm=use_tqdm)
                )

        if sem:
            with sem:
                completed = await asyncio.gather(*tasks)
        else:
            completed = await asyncio.gather(*tasks)

    print(f'Completed {sum(completed)}/{len(tasks)} tasks.')

    return sum(completed) == len(tasks)