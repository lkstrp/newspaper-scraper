# Newspaper-Scraper  
  
##### The all-in-one Python package for seamless newspaper article indexing, scraping, and processing – supports public and premium content!
  
## Intro  
While tools like [newspaper3k](https://newspaper.readthedocs.io/en/latest/) and [goose3](https://github.com/goose3/goose3) can be used for extracting articles from news websites, they need a dedicated article url for older articles and do not support paywall content. This package aims to solve these issues by providing a unified interface for indexing, extracting and processing articles from newspapers.  
1. Indexing: Index articles from a newspaper website using the [beautifulsoup](https://beautiful-soup-4.readthedocs.io/en/latest/) package for public articles and [selenium](https://selenium-python.readthedocs.io/) for paywall content.  
2. Extraction: Extract article content using the [goose3](https://github.com/goose3/goose3) package.  
3. Processing: Process articles for nlp features using the [spaCy](https://spacy.io/) package.  
  
The indexing functionality is based on a dedicated file for each newspaper. A few newspapers are already supported, but it is easy to add new ones.  
  
### Supported Newspapers  
| Logo | Newspaper                                        | Country | Time span  | Number of articles |  
| ----------------------------------------------------------------------------------------------------------------------------------------------- |--------------------------------------------------| ------- |------------| --------------- |  
| <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/4/48/Der_Spiegel_2022_logo.svg/640px-Der_Spiegel_2022_logo.svg.png" height="70"> | [Der Spiegel](https://www.spiegel.de/)           | Germany | Since 2000 | tbd |  
| <img src="https://upload.wikimedia.org/wikipedia/commons/0/0a/Die_Welt_Logo_2015.png" height="70"> | [Die Welt](https://www.welt.de/)                 | Germany | Since 2000 | tbd  
| <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/e/e3/Logo_BILD.svg/1920px-Logo_BILD.svg.png" height="70"> | [Bild](https://www.bild.de/)                     | Germany | Since 2006 | tbd |  
| <img src="https://upload.wikimedia.org/wikipedia/commons/6/60/Die_Zeit-Logo-Bremen.svg" height="70"> | [Die Zeit](https://www.zeit.de/)                 | Germany | Since 1946 | tbd |   
| <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/ff/Handelsblatt_201x_logo.svg/2880px-Handelsblatt_201x_logo.svg.png" height="70"> | [Handelsblatt](https://www.handelsblatt.com/)    | Germany | Since 2003 | tbd | 
| <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1d/Tagesspiegel_%282022-11-29%29.svg/2880px-Tagesspiegel_%282022-11-29%29.svg.png" height="70"> | [Der Tagesspiegel](https://www.tagesspiegel.de/) | Germany | Since 2000 | tbd |
| <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/4/42/S%C3%BCddeutsche_Zeitung_Logo.svg/2880px-S%C3%BCddeutsche_Zeitung_Logo.svg.png" height="70"> | [Süddeutsche Zeitung](https://www.sueddeutsche.de/)    | Germany | Since 2001 | tbd |

## Setup  
It is recommended to install the package in an dedicated Python environment.  
To install the package via pip, run the following command:  
  
```bash  
pip install newspaper-scraper
```  
  
To also include the nlp extraction functionality (via [spaCy](https://spacy.io/)), run the following command:  
  
```bash  
pip install newspaper-scraper[nlp]
```  
  
## Usage  
To index, extract and process all public and premium articles from [Der Spiegel](https://www.spiegel.de/), published in August 2021, run the following code:  
  
```python  
import newspaper_scraper as nps  
from credentials import username, password  
  
with nps.Spiegel(db_file='articles.db') as news:
    news.index_articles_by_date_range('2021-08-01', '2021-08-31')  
    news.scrape_public_articles()
    news.scrape_premium_articles(username=username, password=password)  
    news.nlp()
```  
  
This will create a sqlite database file called `articles.db` in the current working directory. The database contains the following tables:  
- `tblArticlesIndexed`: Contains all indexed articles with their scraping/ processing status and whether they are public or premium content.  
- `tblArticlesScraped`: Contains metadata for all parsed articles, provided by goose3.  
- `tblArticlesProcessed`: Contains nlp features of the cleaned article text, provided by spaCy.
