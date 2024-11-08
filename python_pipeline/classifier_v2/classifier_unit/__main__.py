if __name__ == '__main__':
    from . import util
    from . import client

    config = util.read_config()
    util.setup_logging(config, "client")
    client = client.ClassifierPipelineClient(config)
    client.run()
