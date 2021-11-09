import argparse
import json
import sys

from datetime import datetime
from kafka import KafkaConsumer


predicted_alerts = set()
n_seen_alerts = 0
n_successful_predictions = 0


def write_statistics(out_file=sys.stdout):
    out_file.write(
        f"Total alerts observed: {n_seen_alerts}\n" \
        f"Total predictions: {len(predicted_alerts)}\n" \
        f"Successful predictions: {n_successful_predictions}")


if __name__ == "__main__":
    argp = argparse.ArgumentParser()
    argp.add_argument("--input-topic", default="aggregated")
    argp.add_argument("--predictions-topic", default="predictions")
    args = argp.parse_args()
    
    out_file_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    outfile = open(out_file_name, "w")

    input_topic = args.input_topic
    predictions_topic = args.predictions_topic

    consumer = KafkaConsumer()
    consumer.subscribe(topics=[input_topic, predictions_topic])

    for message in consumer:
        if message.topic == input_topic:
            alert = json.loads(message.value)
            n_seen_alerts += 1
            if alert in predicted_alerts:
                n_successful_predictions += 1
        elif message.topic == predictions_topic:
            alert = json.loads(message.value)
            predicted_alerts.add(alert)
        write_statistics(outfile)
    outfile.close()
