import unittest
from tests.test_flatten_ga_data import TestCFFlattenMethods
from tests.test_input_validator import TestInputValidator
from tests.test_generate_config_cf import TestGenerateConfigCf
from tests.test_generate_config_lm import TestGenerateConfigLm
from tests.test_generate_config_lr import TestGenerateConfigLr
from tests.test_generate_config_pst import TestGenerateConfigPst
from tests.test_generate_config_b import TestGenerateConfigB
from tests.test_build_ga_flattener_config import TestCFBuildFlattenerGaDatasetConfig


if __name__ == '__main__':
    test_suite = unittest.TestSuite()

    # tests
    test_suite.addTest(unittest.makeSuite(TestGenerateConfigCf))
    test_suite.addTest(unittest.makeSuite(TestGenerateConfigLm))
    test_suite.addTest(unittest.makeSuite(TestGenerateConfigLr))
    test_suite.addTest(unittest.makeSuite(TestGenerateConfigPst))
    test_suite.addTest(unittest.makeSuite(TestGenerateConfigB))
    test_suite.addTest(unittest.makeSuite(TestCFBuildFlattenerGaDatasetConfig))
    test_suite.addTest(unittest.makeSuite(TestInputValidator))
    test_suite.addTest(unittest.makeSuite(TestCFFlattenMethods))

    runner = unittest.TextTestRunner()
    runner.run(test_suite)