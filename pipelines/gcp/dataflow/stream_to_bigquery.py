"""Apache Beam: Pub/Sub HMIS events -> BigQuery (raw + curated).

Mirrors the AWS/Kafka path on GCP. DirectRunner locally, DataflowRunner in GCP.
"""
from __future__ import annotations

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT = "your-gcp-project"
DATASET_RAW = "hmis_raw"


class ParseEvent(beam.DoFn):
    def process(self, element):
        try:
            ev = json.loads(element.decode("utf-8"))
            yield {
                "event_id":   ev.get("event_id"),
                "event_type": ev.get("event_type") or ev.get("encounter_type"),
                "client_id":  ev.get("client_id"),
                "project_id": ev.get("project_id"),
                "event_time": ev.get("event_time"),
                "payload":    json.dumps(ev),
            }
        except Exception as exc:
            logging.warning("dlq: %s", exc)
            yield beam.pvalue.TaggedOutput("dlq", element)


def run(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--subscription",
                   default=f"projects/{PROJECT}/subscriptions/hmis-events-sub")
    p.add_argument("--runner", default="DirectRunner")
    args, beam_args = p.parse_known_args(argv)

    opts = PipelineOptions(beam_args, runner=args.runner, streaming=True)
    with beam.Pipeline(options=opts) as pipe:
        (
            pipe
            | "Read" >> beam.io.ReadFromPubSub(subscription=args.subscription)
            | "Parse" >> beam.ParDo(ParseEvent()).with_outputs("dlq", main="main").main
            | "ToBQ" >> beam.io.WriteToBigQuery(
                table=f"{PROJECT}:{DATASET_RAW}.events",
                schema="event_id:STRING,event_type:STRING,client_id:STRING,project_id:STRING,event_time:TIMESTAMP,payload:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
