from prometheus_client import Gauge

class PrometheusConnector:
    def __init__(self):
        self.current_block = Gauge('current_block_height', 'Current block height')
        self.target_block = Gauge('target_block_height', 'Target block height')
        self.last_synced_block = Gauge('last_synced_block_height', 'Last synced block height')
        self.blocks_to_sync = Gauge('blocks_to_sync', 'Number of Blocks To Sync')
