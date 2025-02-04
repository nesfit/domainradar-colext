import domrad_kafka_client
from . import classifier_impl

def main():
    domrad_kafka_client.run_client("feature_vectors", classifier_impl.ClassifierProcessor)


if __name__ == '__main__':
    main()
