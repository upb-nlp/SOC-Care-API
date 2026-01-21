import subprocess
import json
import re
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent

SPIDERS = [
    {
        "dir": "thehackernews_spider",
        "command": ["scrapy", "crawl", "thehackernews", "-o", "thehackernews.json"],
        "output": "thehackernews.json",
        "source": "The Hacker News",
    },
    {
        "dir": "securityweek",
        "command": ["scrapy", "crawl", "securityweek", "-o", "securityweek.json"],
        "output": "securityweek.json",
        "source": "SecurityWeek",
    },
    {
        "dir": "bleeping_spider",
        "command": ["scrapy", "crawl", "bleeping", "-o", "bleeping.json"],
        "output": "bleeping.json",
        "source": "BleepingComputer",
    },
]

timestamp = datetime.now().strftime("%Y%m%d")

# ------------------------
# Step A: Run spiders
# ------------------------

for spider in SPIDERS:
    print(f"Running {spider['source']} spider...")
    subprocess.run(
        spider["command"],
        cwd=BASE_DIR / spider["dir"],
        check=True
    )

# ------------------------
# Step B: Concatenate + enrich
# ------------------------

all_items = []
article_id = 1

for spider in SPIDERS:
    json_path = BASE_DIR / spider["dir"] / spider["output"]

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

        for item in data:
            item["id"] = article_id
            item["source"] = spider["source"]
            article_id += 1
            all_items.append(item)

# ------------------------
# Step B.1: Cleanup individual spider JSONs
# ------------------------

for spider in SPIDERS:
    json_path = BASE_DIR / spider["dir"] / spider["output"]

    if json_path.exists():
        json_path.unlink()
        print(f"Deleted {json_path}")

# ------------------------
# Step C: Post-processing
# ------------------------

for item in all_items:
    if "body" in item and isinstance(item["body"], str):
        item["body"] = re.sub(r'(?<![.!?])\n', ' ', item["body"])
        item["body"] = re.sub(r'[\t\r]', '', item["body"])
        item["body"] = re.sub(r'\n+', '\n', item["body"])
        item["body"] = item["body"].replace(u'\xa0', u' ')

# Create a directory for output files if it doesn't exist
output_dir_path = f"SOC-Care-API/{timestamp}_outputs"
Path(output_dir_path).mkdir(parents=True, exist_ok=True)

with open(f"{output_dir_path}/{timestamp}_combined.json", "w", encoding="utf-8") as f:
    json.dump(all_items, f, ensure_ascii=False, indent=4)

# -----------------------------------------------------
# TOKEN CLASSIFICATION PART
# -----------------------------------------------------

from api_inference_token_classification_model import TokenClassificationSecurityModel
from api_visualization_table import EntityTableCSVExporter

ner = TokenClassificationSecurityModel("finetuned_CTI_BERT_soccare", device='cuda')
table_builder = EntityTableCSVExporter()


payload = [item["body"] for item in all_items]

result = ner.generate(payload)

for i, res in enumerate(result):
    table_builder.export(
        unique=True,
        pred_spans=result[i]["pred_spans"],
        output_file=f"{output_dir_path}/{all_items[i]['id']}.csv",
)