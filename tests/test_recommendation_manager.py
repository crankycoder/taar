from taar.profile_fetcher import ProfileFetcher
from taar.recommenders import RecommendationManager
from taar.recommenders.base_recommender import BaseRecommender
from .test_similarityrecommender import mock_s3_categorical_data   # noqa
from .mocks import MockProfileController, MockRecommenderFactory


class StubRecommender(BaseRecommender):
    """ A shared, stub recommender that can be used for testing.
    """
    def __init__(self, can_recommend, stub_recommendations):
        self._can_recommend = can_recommend
        self._recommendations = stub_recommendations

    def can_recommend(self, client_info, extra_data={}):
        return self._can_recommend

    def recommend(self, client_data, limit, extra_data={}):
        return self._recommendations


def test_none_profile_returns_empty_list():
    fetcher = ProfileFetcher(MockProfileController(None))
    factory = MockRecommenderFactory()
    rec_manager = RecommendationManager(factory, fetcher)
    assert rec_manager.recommend("random-client-id", 10) == []


def test_recommendation_strategy():
    """The recommendation manager is currently very naive and just
    selects the first recommender which returns 'True' to
    can_recommend()."""
    EXPECTED_ADDONS = ["expected_id", "other-id"]

    # Create a stub ProfileFetcher that always returns the same
    # client data.
    class StubFetcher:
        def get(self, client_id):
            return {'client_id': '00000'}

    # Configure the recommender so that only the second model
    # can recommend and return the expected addons.
    factory = MockRecommenderFactory(legacy=lambda: StubRecommender(False, []),
                                     collaborative=lambda: StubRecommender(True, EXPECTED_ADDONS),
                                     similarity=lambda: StubRecommender(False, []),
                                     locale=lambda: StubRecommender(False, []))

    # Make sure the recommender returns the expected addons.
    manager = RecommendationManager(factory, StubFetcher())
    results = manager.recommend("client-id",
                                10,
                                extra_data={'branch': 'linear'})
    assert results == EXPECTED_ADDONS
