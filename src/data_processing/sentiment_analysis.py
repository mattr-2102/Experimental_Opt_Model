from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

class SentimentAnalyzer:
    def __init__(self, model_name="yiyanghkust/finbert-tone"):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)

    def analyze_sentiment(self, text):
        """Perform sentiment analysis on financial news."""
        if not text or not isinstance(text, str):
            print("⚠️ Invalid input text. Skipping sentiment analysis.")
            return None

        inputs = self.tokenizer(text, return_tensors="pt")
        outputs = self.model(**inputs)
        return torch.softmax(outputs.logits, dim=1).detach().numpy()