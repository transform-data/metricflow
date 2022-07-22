from metricflow.inference.models import InferenceSignalType


def test_inference_type_node_conflict():
    """Make sure the inference signal type hierarchy is correctly configured."""

    # IDENTIFIER
    assert InferenceSignalType.ID.UNKNOWN.is_descendant(InferenceSignalType.UNKNOWN)
    assert not InferenceSignalType.ID.UNKNOWN.is_descendant(InferenceSignalType.DIMENSION.UNKNOWN)
    assert not InferenceSignalType.ID.UNKNOWN.is_descendant(InferenceSignalType.MEASURE.UNKNOWN)

    assert InferenceSignalType.ID.UNIQUE.is_descendant(InferenceSignalType.ID.UNKNOWN)
    assert InferenceSignalType.ID.FOREIGN.is_descendant(InferenceSignalType.ID.UNKNOWN)
    assert InferenceSignalType.ID.PRIMARY.is_descendant(InferenceSignalType.ID.UNIQUE)

    # DIMENSION
    assert InferenceSignalType.DIMENSION.UNKNOWN.is_descendant(InferenceSignalType.UNKNOWN)
    assert not InferenceSignalType.DIMENSION.UNKNOWN.is_descendant(InferenceSignalType.ID.UNKNOWN)
    assert not InferenceSignalType.DIMENSION.UNKNOWN.is_descendant(InferenceSignalType.MEASURE.UNKNOWN)

    assert InferenceSignalType.DIMENSION.CATEGORICAL.is_descendant(InferenceSignalType.DIMENSION.UNKNOWN)
    assert InferenceSignalType.DIMENSION.TIME.is_descendant(InferenceSignalType.DIMENSION.UNKNOWN)

    # MEASURE
    assert InferenceSignalType.MEASURE.UNKNOWN.is_descendant(InferenceSignalType.UNKNOWN)
    assert not InferenceSignalType.MEASURE.UNKNOWN.is_descendant(InferenceSignalType.ID.UNKNOWN)
    assert not InferenceSignalType.MEASURE.UNKNOWN.is_descendant(InferenceSignalType.DIMENSION.UNKNOWN)
