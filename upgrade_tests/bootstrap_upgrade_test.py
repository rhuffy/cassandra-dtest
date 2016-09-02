from bootstrap_test import BaseBootstrapTest
from tools.decorators import since, no_vnodes


class TestBootstrapUpgrade(BaseBootstrapTest):
    __test__ = True

    """
    @jira_ticket CASSANDRA-11841
    Test that bootstrap works with a mixed version cluster
    In particular, we want to test that keep-alive is not sent
    to a node with version < 3.10
    """
    @no_vnodes()
    @since('3.10', max_version='3.99')
    def simple_bootstrap_test_mixed_versions(self):
        self._base_bootstrap_test(bootstrap_from_version="3.5")
