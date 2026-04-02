"""
Relevance + Stance Classifier

Usage:
    python classify_relevance_and_stance.py <path_to_csv> <path_to_model_dir> [output_path]

Arguments:
    path_to_csv      Path to the input CSV file. Must have columns: title, description, sponsor_parties
    path_to_model_dir  Path to the saved LegalBERT relevance model directory
    output_path      (Optional) Path for the output CSV. Defaults to 'bills_classified.csv'
"""

import sys
import json
import torch
import numpy as np
import pandas as pd
from pathlib import Path
from transformers import AutoTokenizer, BertModel
from torch.utils.data import DataLoader
from tqdm.auto import tqdm


# ---------- Model Definition ----------

class LegalBERTClassifier(torch.nn.Module):
    def __init__(self, bert_model, hidden_size=768, dropout=0.3):
        super().__init__()
        self.bert = bert_model
        self.dropout = torch.nn.Dropout(dropout)
        self.linear = torch.nn.Linear(hidden_size, 1)

    def forward(self, input_ids, attention_mask=None):
        bert_output = self.bert(input_ids=input_ids, attention_mask=attention_mask).last_hidden_state
        cls_output = bert_output[:, 0, :]
        cls_output = self.dropout(cls_output)
        logit = self.linear(cls_output)
        return torch.squeeze(logit)


# ---------- Inference ----------

@torch.no_grad()
def predict_relevance(relevance_model, tokenizer, device, texts, batch_size=16, max_length=256, threshold=0.5):
    relevance_model.eval()
    probs = []

    loader = DataLoader(list(texts), batch_size=batch_size, shuffle=False)

    for batch_texts in tqdm(loader, desc="Running LegalBERT relevance inference"):
        enc = tokenizer(
            list(batch_texts),
            padding=True,
            truncation=True,
            max_length=max_length,
            return_tensors="pt"
        ).to(device)

        logits = relevance_model(enc["input_ids"], enc["attention_mask"])
        batch_probs = torch.sigmoid(logits).cpu().numpy()
        probs.append(batch_probs)

    probs = np.concatenate(probs)
    preds = (probs >= threshold).astype(int)

    return probs, preds


# ---------- Feature Engineering ----------

def build_bill_text(df):
    if "description" in df.columns:
        return (df["title"].fillna("") + " " + df["description"].fillna("")).str.strip()
    return df["title"].fillna("")


def parse_party_counts(party_str):
    """Parse 'R | D | R | R' into party counts."""
    if pd.isna(party_str):
        return {'R': 0, 'D': 0, 'Other': 0}

    parties = [p.strip() for p in str(party_str).split('|')]
    counts = {
        'R': sum(1 for p in parties if p == 'R'),
        'D': sum(1 for p in parties if p == 'D'),
        'Other': sum(1 for p in parties if p not in ('R', 'D'))
    }
    return counts


def bill_dominant_party(row):
    if row['r_sponsors'] > row['d_sponsors']:
        return 'R'
    elif row['d_sponsors'] > row['r_sponsors']:
        return 'D'
    elif row['r_sponsors'] == 0 and row['d_sponsors'] == 0:
        return 'Other'
    else:
        return 'Bipartisan'


# ---------- Main ----------

def main(path_to_file, rel_dir, output_path="bills_classified.csv"):
    rel_dir = Path(rel_dir)

    # Read data
    print(f"Reading data from: {path_to_file}")
    df = pd.read_csv(path_to_file, engine='python', on_bad_lines='skip')

    # Set up device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # Load relevance model
    print(f"Loading relevance model from: {rel_dir}")
    ckpt = torch.load(rel_dir / "relevance_model.pt", map_location=device)

    tokenizer = AutoTokenizer.from_pretrained(rel_dir)
    base_model = BertModel.from_pretrained(ckpt["base_model_name"]).to(device)

    relevance_model = LegalBERTClassifier(
        base_model,
        hidden_size=ckpt["hidden_size"],
        dropout=ckpt["dropout"]
    ).to(device)

    relevance_model.load_state_dict(ckpt["model_state_dict"])
    relevance_model.eval()

    # Build bill text
    df["bill_text"] = build_bill_text(df)

    # Predict relevance
    print("Running relevance classification...")
    df["relevance_prob"], df["relevance_pred"] = predict_relevance(
        relevance_model, tokenizer, device, df["bill_text"], threshold=0.6
    )

    # Parse sponsor parties
    party_counts = df['sponsor_parties'].apply(parse_party_counts).apply(pd.Series)
    df['r_sponsors'] = party_counts['R']
    df['d_sponsors'] = party_counts['D']
    df['other_sponsors'] = party_counts['Other']

    df['bill_dominant_party'] = df.apply(bill_dominant_party, axis=1)

    # Apply stance rules
    stance_rules = {
        "R": "harmful",
        "D": "supportive",
        "Other": "ambiguous",
        "Bipartisan": "ambiguous"
    }

    df["pred_stance"] = np.where(
        df["relevance_pred"] == 1,
        df["bill_dominant_party"].map(stance_rules),
        "not applicable"
    )

    # Save results
    print(f"Saving classified results to: {output_path}")
    df.to_csv(output_path, index=False)
    print("Done.")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    path_to_file = sys.argv[1]
    rel_dir = sys.argv[2]
    output_path = sys.argv[3] if len(sys.argv) > 3 else "bills_classified.csv"

    main(path_to_file, rel_dir, output_path)
