import logging

from src.rules.cross_reference_value_check import CrossReferenceValueCheck
from src.rules.data_comparator import DataComparator
from src.rules.length_check import LengthCheck
from src.rules.null_check import NullCheck
from src.rules.range_check import RangeCheck
from src.rules.reference_values_check import ReferenceValuesCheck
from src.rules.regex_check import RegexCheck
from src.rules.sql_validator import SqlValidator
from src.rules.uniqueness_check import UniquenessCheck
from src.rules.whole_number_check import WholeNumberCheck


class RuleExecutorFactory:
    def __init__(self, context):
        self.context = context

    def get_rule_executor(self):
        template_name = self.context.get_rule_template_name()
        executor = None
        if template_name == 'DATA_DIFF':
            executor = DataComparator(self.context)
        if template_name == 'SQL_VALIDATOR':
            executor = SqlValidator(self.context)
        if template_name == 'RANGE_CHECK':
            executor = RangeCheck(self.context)
        if template_name == 'NULL_CHECK':
            executor = NullCheck(self.context)
        if template_name == 'LENGTH_CHECK':
            executor = LengthCheck(self.context)
        if template_name == 'REFERENCE_VALUES_CHECK':
            executor = ReferenceValuesCheck(self.context)
        if template_name == 'UNIQUENESS_CHECK':
            executor = UniquenessCheck(self.context)
        if template_name == 'WHOLE_NUMBER_CHECK':
            executor = WholeNumberCheck(self.context)
        if template_name == 'CROSS_REFERENCE_VALUES_CHECK':
            executor = CrossReferenceValueCheck(self.context)
        if template_name == 'REGEX_CHECK':
            executor = RegexCheck(self.context)

        return executor
