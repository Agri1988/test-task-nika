import os
import re
from dataclasses import dataclass
from typing import List, Type, TypeVar, Optional, Dict, Callable

from httpx import AsyncClient, Response, Limits, RequestError, RemoteProtocolError, HTTPStatusError, ConnectError
import asyncio
from bs4 import BeautifulSoup as BS, ResultSet, Tag

from db import insert_medical_codes


REST = int(os.environ.get("REST", 180))


@dataclass
class Entry:
    name: str
    url: str
    description: str


E = TypeVar("E", bound=Entry)


@dataclass
class Category(Entry):
    pass


@dataclass
class SubCategory(Entry):
    category: Optional[Category] = None


@dataclass
class Disease(Entry):
    subcategory: Optional = None


def print_counter_decorator(func: Callable) -> Callable:
    counter = 1

    def wrapped(*args, **kwargs):
        nonlocal counter
        returned_data = func(*args, **kwargs)
        print(counter, returned_data, sep="\t")
        counter += 1
        return returned_data
    return wrapped


async def get_page_content(page_url: str, client: AsyncClient, attempt: int = 1) -> [None, None, str]:
    try:
        response: Response = await client.get(page_url, timeout=None)
        if not response.is_success:
            raise HTTPStatusError(
                f"Response status code {response.status_code}", request=response.request, response=response
            )
        return response.text
    except (RemoteProtocolError, HTTPStatusError, ConnectError) as exc:
        print(
            f"Attempt {attempt} to request {page_url}. "
            f"Got {exc}, "
            f"going to sleep {REST} seconds and will try again...."
        )
        counter = 0
        while counter < REST:
            counter += 1
            await asyncio.sleep(1)
        if attempt < 10:
            print(f"Retry request to {page_url}")
            return await get_page_content(page_url=page_url, client=client, attempt=attempt + 1)
        else:
            raise RequestError(f"{exc}")


def get_soup_object(raw_content: str) -> BS:
    return BS(raw_content)


@print_counter_decorator
def get_data_from_list_tag(entry: Tag, entry_class: Type[E]) -> E:
    link = entry.find("a", href=True)
    description = entry.text
    return entry_class(
        name=link.text,
        url=link.get("href"),
        description=re.sub("^\s*(\w\d{1,2}-?){1,2}\s*|\s*$", "", description),
    )


def get_entry_list(
    page_bs: BS, entry_class: Type[E], list_selector: Optional = None
) -> List[E]:
    if not list_selector:
        list_selector = ["ul"]
    else:
        list_selector = ["ul", list_selector]
    entry_list: ResultSet[Tag] = (
        page_bs.find("div", {"class": "body-content"})
        .find(*list_selector)
        .find_all("li")
    )
    return [
        get_data_from_list_tag(entry=entry, entry_class=entry_class)
        for entry in entry_list
    ]


async def create_entry_list(
    url: str,
    client: AsyncClient,
    entry_class: Type[E],
    list_selector: Optional[dict] = None,
    parent: Optional[E] = None,
) -> [None, None, List[E]]:
    raw_page_content = await get_page_content(page_url=url, client=client)
    page_bs = get_soup_object(raw_content=raw_page_content)
    entry_list = get_entry_list(
        page_bs=page_bs, entry_class=entry_class, list_selector=list_selector
    )
    if parent:
        for entry in entry_list:
            setattr(entry, parent.__class__.__name__.lower(), parent)
    return entry_list


async def get_child_entries(
    parent_list: List[E],
    entry_class: Type[E],
    domain: str,
    client: AsyncClient,
    list_selector: Dict[str, str],
) -> List[Disease]:
    tasks = []
    for parent in parent_list:
        url = f"{domain}{parent.url}"
        tasks.append(
            asyncio.create_task(
                create_entry_list(
                    url=url,
                    client=client,
                    entry_class=entry_class,
                    list_selector=list_selector,
                    parent=parent,
                )
            )
        )
    result: List[List[E]] = await asyncio.gather(*tasks)
    entry_list = [entry for entry_lst in result for entry in entry_lst]
    return entry_list


def disease_to_medical_code_tuple(disease_list: List[Disease]) -> tuple:
    return tuple(
        (
            disease.subcategory.category.name,
            disease.subcategory.category.description,
            disease.name,
            disease.description,
        )
        for disease in disease_list
    )


async def main():
    limits = Limits(max_connections=20)
    headers = {
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
        ),
        "accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.9"
        )
    }
    async with AsyncClient(limits=limits, headers=headers) as client:
        domain = "https://www.icd10data.com"
        main_page_url = f"{domain}/ICD10CM/Codes"
        categories = await create_entry_list(
            url=main_page_url,
            client=client,
            entry_class=Category,
        )
        print(categories)
        subcategory_list = await get_child_entries(
            parent_list=categories,
            entry_class=SubCategory,
            domain=domain,
            client=client,
            list_selector={"class": "i51"},
        )
        print(subcategory_list)
        disease_list = await get_child_entries(
            parent_list=subcategory_list,
            entry_class=Disease,
            domain=domain,
            client=client,
            list_selector={"class": "i51"},
        )
        print(disease_list)
        medical_codes = disease_to_medical_code_tuple(disease_list=disease_list)
        insert_medical_codes(medical_codes=medical_codes)


if __name__ == "__main__":
    asyncio.run(main())
