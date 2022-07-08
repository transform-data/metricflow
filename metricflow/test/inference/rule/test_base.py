from metricflow.inference.rule.base import InferenceSignalType


def test_inference_signal_type_conflict():
    """Test `InferenceSignalType.conflict` by asserting it is commutative, i.e, conflict(a,b) == conflict(b,a) and it returns correct results."""

    def conflict_commutative(a: InferenceSignalType, b: InferenceSignalType) -> bool:
        """Assert the results of conflict(a, b) are the same as conflict(b, a). If passes assertion, returns the result"""
        a_b = InferenceSignalType.conflict(a, b)
        b_a = InferenceSignalType.conflict(b, a)
        assert a_b == b_a
        return a_b

    # Make this test break if new entries are added to InferenceSignalType enum.
    # Maybe this is not a very good solution, but we wanted to make sure all possible
    # conflicts are covered, given that a policy thinking incompatible types are
    # compatible or vice versa could be the reason for weird and untraceable bugs
    # in inference (like IDs not being detected, or being wrongly detected).
    assert len(InferenceSignalType) == 7

    # IDENTIFIER
    assert not conflict_commutative(InferenceSignalType.IDENTIFER, InferenceSignalType.PRIMARY_IDENTIFIER)
    assert not conflict_commutative(InferenceSignalType.IDENTIFER, InferenceSignalType.FOREIGN_IDENTIFIER)
    assert conflict_commutative(InferenceSignalType.IDENTIFER, InferenceSignalType.DIMENSION)
    assert conflict_commutative(InferenceSignalType.IDENTIFER, InferenceSignalType.TIME_DIMENSION)
    assert conflict_commutative(InferenceSignalType.IDENTIFER, InferenceSignalType.CATEGORICAL_DIMENSION)
    assert conflict_commutative(InferenceSignalType.IDENTIFER, InferenceSignalType.MEASURE_FIELD)

    # PRIMARY_IDENTIFIER
    assert conflict_commutative(InferenceSignalType.PRIMARY_IDENTIFIER, InferenceSignalType.FOREIGN_IDENTIFIER)
    assert conflict_commutative(InferenceSignalType.PRIMARY_IDENTIFIER, InferenceSignalType.DIMENSION)
    assert conflict_commutative(InferenceSignalType.PRIMARY_IDENTIFIER, InferenceSignalType.TIME_DIMENSION)
    assert conflict_commutative(InferenceSignalType.PRIMARY_IDENTIFIER, InferenceSignalType.CATEGORICAL_DIMENSION)
    assert conflict_commutative(InferenceSignalType.PRIMARY_IDENTIFIER, InferenceSignalType.MEASURE_FIELD)

    # FOREIGN_IDENTIFIER
    assert conflict_commutative(InferenceSignalType.FOREIGN_IDENTIFIER, InferenceSignalType.DIMENSION)
    assert conflict_commutative(InferenceSignalType.FOREIGN_IDENTIFIER, InferenceSignalType.TIME_DIMENSION)
    assert conflict_commutative(InferenceSignalType.FOREIGN_IDENTIFIER, InferenceSignalType.CATEGORICAL_DIMENSION)
    assert conflict_commutative(InferenceSignalType.FOREIGN_IDENTIFIER, InferenceSignalType.MEASURE_FIELD)

    # DIMENSION
    assert not conflict_commutative(InferenceSignalType.DIMENSION, InferenceSignalType.TIME_DIMENSION)
    assert not conflict_commutative(InferenceSignalType.DIMENSION, InferenceSignalType.CATEGORICAL_DIMENSION)
    assert conflict_commutative(InferenceSignalType.DIMENSION, InferenceSignalType.MEASURE_FIELD)

    # TIME_DIMENSION
    assert conflict_commutative(InferenceSignalType.TIME_DIMENSION, InferenceSignalType.CATEGORICAL_DIMENSION)
    assert conflict_commutative(InferenceSignalType.TIME_DIMENSION, InferenceSignalType.MEASURE_FIELD)

    # CATEGORICAL_DIMENSION
    assert conflict_commutative(InferenceSignalType.CATEGORICAL_DIMENSION, InferenceSignalType.MEASURE_FIELD)

    # MEASURE_FIELD
    # has already been tested in all previous cases since we're testing for commutability.
