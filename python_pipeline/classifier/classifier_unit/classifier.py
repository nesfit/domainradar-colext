from classifiers.pipeline import Pipeline

pipeline = None


def init_pipeline(options):
    global pipeline
    pipeline = Pipeline(options)
