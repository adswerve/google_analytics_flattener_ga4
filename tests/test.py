import unittest
from tests.test_build_ga_flattener_config import TestCFBuildFlattenerGaDatasetConfig
from tests.test_flatten_ga_data import TestCFFlattenMethods, TestCFFlattenMethodsSchemaChangeCollectedTrafficSource, TestCFFlattenMethodsSchemaChangeIsActiveUser
from tests.test_flatten_ga_data_intraday import TestCFFlattenMethodsIntraday
from tests.test_generate_config_b import TestGenerateConfigB
from tests.test_generate_config_cf import TestGenerateConfigCf
from tests.test_generate_config_lm import TestGenerateConfigLm
from tests.test_generate_config_lr import TestGenerateConfigLr
from tests.test_generate_config_pst import TestGenerateConfigPst
from tests.test_input_validator import TestInputValidator, TestInputValidatorConfigurationError
from test_intraday_sql_view import TestCFIntradaySQLView, TestManageIntradaySQLView
from tests.test_manage_intraday_schedule import TestManageIntradayFlatteningSchedule
from tests.test_partitioning import TestPartitioning
from tests.test_valid_resource_names import TestValidResourceNames
from tests.test_verify_queries import TestGenerateQuery
from test_verify_queries_schema_change import TestGenerateQuerySchemaChange1, TestGenerateQuerySchemaChange2

if __name__ == '__main__':
    test_suite = unittest.TestSuite()

    # tests
    test_suite.addTest(unittest.makeSuite(TestCFBuildFlattenerGaDatasetConfig))
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethods))
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethodsSchemaChangeCollectedTrafficSource))
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethodsSchemaChangeIsActiveUser))
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethodsIntraday))
    test_suite.addTest(unittest.makeSuite(TestGenerateConfigB))
    test_suite.addTest(unittest.makeSuite(TestGenerateConfigCf))
    test_suite.addTest(unittest.makeSuite(TestGenerateConfigLm))
    test_suite.addTest(unittest.makeSuite(TestGenerateConfigLr))
    test_suite.addTest(unittest.makeSuite(TestGenerateConfigPst))
    test_suite.addTest(unittest.makeSuite(TestInputValidator))
    test_suite.addTest(unittest.makeSuite(TestCFIntradaySQLView))
    test_suite.addTest(unittest.makeSuite(TestManageIntradaySQLView))
    test_suite.addTest(unittest.makeSuite(TestInputValidatorConfigurationError))
    test_suite.addTest(unittest.makeSuite(TestManageIntradayFlatteningSchedule))
    test_suite.addTest(unittest.makeSuite(TestPartitioning))
    test_suite.addTest(unittest.makeSuite(TestValidResourceNames))
    test_suite.addTest(unittest.makeSuite(TestGenerateQuery))
    test_suite.addTest(unittest.makeSuite(TestGenerateQuerySchemaChange1))
    test_suite.addTest(unittest.makeSuite(TestGenerateQuerySchemaChange2))

    # verbosity: 0 (quiet), 1 (default), 2 (verbose)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(test_suite)
