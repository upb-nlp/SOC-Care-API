import torch
from typing import List, Dict, Any

from transformers import (
    AutoTokenizer,
    AutoModelForTokenClassification,
)

# =========================================================
# Token Classification Security Model Service
# =========================================================

class TokenClassificationSecurityModel:
    def __init__(
        self,
        model_path: str,
        max_tokens: int = 250,
        device: str | None = None,
    ):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.max_tokens = max_tokens

        self.tokenizer = AutoTokenizer.from_pretrained(
            model_path,
            use_fast=True,
            trust_remote_code=True,
        )

        self.model = AutoModelForTokenClassification.from_pretrained(
            model_path,
            trust_remote_code=True,
        ).to(self.device)

        self.model.eval()

        self.id2label = self.model.config.id2label
        self.label2id = self.model.config.label2id

    # -----------------------------------------------------
    # Split text into token chunks
    # -----------------------------------------------------
    def _split_for_inference(self, text: str) -> List[Dict[str, Any]]:
        full = self.tokenizer(
            text,
            return_offsets_mapping=True,
            truncation=False,
            add_special_tokens=False,
        )

        offsets = full["offset_mapping"]
        n_tokens = len(offsets)

        chunks = []
        start_idx = 0

        while start_idx < n_tokens:
            end_idx = min(start_idx + self.max_tokens, n_tokens)
            chunk_offsets = offsets[start_idx:end_idx]

            c0 = chunk_offsets[0][0]
            c1 = chunk_offsets[-1][1]
            chunk_text = text[c0:c1]

            enc = self.tokenizer(
                chunk_text,
                return_offsets_mapping=True,
                return_tensors="pt",
                truncation=True,
                padding=False,
            )

            local_offsets = enc["offset_mapping"].squeeze(0).tolist()
            enc.pop("offset_mapping")

            chunks.append({
                "enc": enc,
                "local_offsets": local_offsets,
                "global_char_start": c0,
            })

            start_idx = end_idx

        return chunks

    # -----------------------------------------------------
    # BIO → character spans
    # -----------------------------------------------------
    @staticmethod
    def _bio_to_char_spans(tags, offsets, text):
        spans = []
        cur_label, cur_start, cur_end = None, None, None

        for tag, (s, e) in zip(tags, offsets):
            if tag == "O":
                if cur_label is not None:
                    spans.append({
                        "start": cur_start,
                        "end": cur_end,
                        "label": cur_label,
                        "text": text[cur_start:cur_end],
                    })
                    cur_label = None
                continue

            pref, lab = tag.split("-", 1)

            if pref == "B" or (cur_label and lab != cur_label):
                if cur_label is not None:
                    spans.append({
                        "start": cur_start,
                        "end": cur_end,
                        "label": cur_label,
                        "text": text[cur_start:cur_end],
                    })
                cur_label, cur_start, cur_end = lab, s, e
            else:
                if cur_label is None:
                    cur_label, cur_start = lab, s
                cur_end = e

        if cur_label:
            spans.append({
                "start": cur_start,
                "end": cur_end,
                "label": cur_label,
                "text": text[cur_start:cur_end],
            })

        return spans

    # -----------------------------------------------------
    # Predict spans for a single text
    # -----------------------------------------------------
    @torch.no_grad()
    def _predict_single(self, text: str) -> Dict[str, Any]:
        chunks = self._split_for_inference(text)

        all_tags = []
        all_offsets = []

        for ch in chunks:
            enc = {k: v.to(self.device) for k, v in ch["enc"].items()}
            logits = self.model(**enc).logits.squeeze(0)
            pred_ids = logits.argmax(-1).tolist()

            base = ch["global_char_start"]
            for pid, (s, e) in zip(pred_ids, ch["local_offsets"]):
                if s == 0 and e == 0:
                    continue
                gs, ge = base + s, base + e
                if gs == ge:
                    continue

                all_tags.append(self.id2label[int(pid)])
                all_offsets.append((gs, ge))

        spans = self._bio_to_char_spans(all_tags, all_offsets, text)

        return {
            "text": text,
            "pred_spans": spans,
        }

    # -----------------------------------------------------
    # PUBLIC API METHOD
    # -----------------------------------------------------
    def generate(self, texts: List[str]) -> List[Dict[str, Any]]:
        """
        API entrypoint.
        Input:
            texts = ["some text", "another text"]
        Output:
            [
              {
                "text": "...",
                "pred_spans": [
                    {"start": 0, "end": 4, "label": "ORG", "text": "ACME"}
                ]
              }
            ]
        """
        return [self._predict_single(t) for t in texts]
