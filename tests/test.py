import unittest
from tests.test_build_ga_flattener_config import TestCFBuildFlattenerGaDatasetConfig
from tests.test_flatten_ga_data_1_events import (TestCFFlattenMethods,
                                                 TestCFFlattenMethodsSchemaChangeCollectedTrafficSource,
                                                 TestCFFlattenMethodsSchemaChangeIsActiveUser,
                                                 )

from tests.test_flatten_ga_data_2_pseudo_users import TestCFFlattenMethodsPseudoUsers, TestCFFlattenMethodsPseudoUsersSourceDoesNotExist
from tests.test_flatten_ga_data_3_users import TestCFFlattenMethodsUsers, TestCFFlattenMethodsUsersSourceDoesNotExist
from tests.test_flatten_ga_data_4_intraday import TestCFFlattenMethodsIntraday

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

from tests.test_verify_queries_events import TestGenerateQuerySourceTableEvents
from tests.test_verify_queries_pseudo_users import TestGenerateQuerySourceTablePseudoUsers
from tests.test_verify_queries_users import TestGenerateQuerySourceTableUsers


if __name__ == '__main__':
    test_suite = unittest.TestSuite()

    # tests
    test_suite.addTest(unittest.makeSuite(TestCFBuildFlattenerGaDatasetConfig))

    # FLATTEN tests start
    # events
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethods))
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethodsSchemaChangeCollectedTrafficSource))
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethodsSchemaChangeIsActiveUser))

    # pseudo users
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethodsPseudoUsers))
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethodsPseudoUsersSourceDoesNotExist))

    # users
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethodsUsers))
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethodsUsersSourceDoesNotExist))

    # intraday
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethodsIntraday))
    # FLATTEN tests end

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

    test_suite.addTest(unittest.makeSuite(TestGenerateQuerySourceTableEvents))
    test_suite.addTest(unittest.makeSuite(TestGenerateQuerySourceTablePseudoUsers))
    test_suite.addTest(unittest.makeSuite(TestGenerateQuerySourceTableUsers))

    # verbosity: 0 (quiet), 1 (default), 2 (verbose)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(test_suite)
