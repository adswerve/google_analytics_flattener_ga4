from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage


class TestCFFlattenMethods(BaseUnitTest):
    
    def test_flatten_ga_data(self):
        c = Context()
        ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                           dataset=BaseUnitTest.DATASET,
                                           table_name=BaseUnitTest.TABLE_TYPE,
                                           date_shard=BaseUnitTest.DATE,
                                           )
        ga_source.run_query_job(query=ga_source.get_session_query(),table_type="ga_flat_sessions")
        ga_source.run_query_job(query=ga_source.get_hit_query(), table_type="ga_flat_hits")
        ga_source.run_query_job(query=ga_source.get_hit_product_query(), table_type="ga_flat_products")
        ga_source.run_query_job(query=ga_source.get_hit_experiment_query(), table_type="ga_flat_experiments")
        ga_source.run_query_job(query=ga_source.get_hit_promotion_query(), table_type="ga_flat_promotions")
        self.assertTrue(True)
