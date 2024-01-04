from textblob import TextBlob
import json
import matplotlib.pyplot as plt
import pandas as pd
import time


def get_data():
    # Specify the path to your JSON file
    date = "2023-12-19T14"
    path = f"../../aws_ingestion/space-news-data-all-{date}.json"

    with open(path, 'r') as json_file:
        base_data = json.load(json_file)

    selected_data = []
    for category, items in base_data.items():
        for item in items:
            attributes = {
                "title": item.get("title"),
                "url": item.get("url"),
                "image_url": item.get("image_url"),
                "news_site": item.get("news_site"),
                "summary": item.get("summary"),
                "published_at": item.get("published_at"),
                "category": category,
            }
            selected_data.append(attributes)
    return selected_data


# Create a function to get sentiment scores
def get_sentiment(text):
    analysis = TextBlob(text)
    return analysis.sentiment.polarity


def show_sentiment_trend(dataset):
    # Convert the results to a DataFrame for easier analysis
    df = pd.DataFrame(dataset)
    df.set_index('published_at', inplace=True)

    # Visualize the sentiment trend
    df.plot(title='Sentiment Trend Over Time', figsize=(10, 6))
    plt.xlabel('Published At')
    plt.ylabel('Sentiment Score')
    plt.legend()
    plt.show()


if __name__ == "__main__":
    start_time = time.time()

    data = get_data()

    # Apply sentiment analysis and store results
    for item in data:
        item["sentiment"] = get_sentiment(item['summary'])

    show_sentiment_trend(data)

    end_time = time.time()
    execution_time_ms = (end_time - start_time) * 1000
    print(f"Execution time: {execution_time_ms} ms")
