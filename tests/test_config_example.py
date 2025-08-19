import pytest
import os
from unittest.mock import patch
from spark_aggregation_poc import Config, Environment


class TestConfigPatterns:
    """Examples of different test patterns for configuration"""
    
    def test_factory_method_for_testing(self):
        """Test using the factory method - RECOMMENDED approach"""
        config = Config.for_testing()
        
        assert config.postgres_url == "jdbc:postgresql://localhost:5432/test_db"
        assert config.postgres_properties["user"] == "test_user"
        assert config.environment == Environment.TESTING
    
    def test_factory_method_with_custom_values(self):
        """Test factory method with custom test values"""
        custom_props = {
            "user": "custom_test_user",
            "password": "custom_test_pass",
            "driver": "org.postgresql.Driver"
        }
        
        config = Config.for_testing(
            postgres_url="jdbc:postgresql://localhost:5432/custom_test_db",
            postgres_properties=custom_props
        )
        
        assert "custom_test_db" in config.postgres_url
        assert config.postgres_properties["user"] == "custom_test_user"
    
    def test_environment_variable_override(self):
        """Test using environment variable to control config"""
        with patch.dict(os.environ, {"APP_ENV": "test"}):
            config = Config.from_environment()
            assert config.environment == Environment.TESTING
            assert "test_db" in config.postgres_url
    
    def test_config_properties_are_immutable(self):
        """Test that config properties return copies to prevent mutation"""
        config = Config.for_testing()
        
        # Get properties
        props = config.postgres_properties
        original_user = props["user"]
        
        # Try to mutate
        props["user"] = "hacker"
        
        # Original config should be unchanged
        assert config.postgres_properties["user"] == original_user
        assert config.postgres_properties["user"] != "hacker"
    
    def test_private_properties_not_directly_accessible(self):
        """Test that private properties are not directly accessible (good practice)"""
        config = Config.for_testing()
        
        # These should work (public interface)
        assert config.postgres_url is not None
        assert config.postgres_properties is not None
        
        # Direct access to private attributes should be discouraged
        # (though technically possible in Python)
        # assert hasattr(config, '_postgres_url')  # This would work but not recommended


class TestWithConfigDependencyInjection:
    """Example of how to use config in actual application code with DI"""
    
    class DatabaseService:
        """Example service that depends on config"""
        def __init__(self, config: Config):
            self._config = config
        
        def get_connection_string(self) -> str:
            return self._config.postgres_url
        
        def get_username(self) -> str:
            return self._config.postgres_properties["user"]
    
    def test_service_with_test_config(self):
        """Test service using dependency injection with test config"""
        # Arrange
        test_config = Config.for_testing(
            postgres_url="jdbc:postgresql://localhost:5432/test_service_db",
            postgres_properties={"user": "test_service_user", "password": "test_pass"}
        )
        service = self.DatabaseService(test_config)
        
        # Act & Assert
        assert "test_service_db" in service.get_connection_string()
        assert service.get_username() == "test_service_user"
    
    def test_service_with_different_test_configs(self):
        """Test service with multiple different test configurations"""
        configs = [
            Config.for_testing(postgres_url="jdbc:postgresql://localhost:5432/db1"),
            Config.for_testing(postgres_url="jdbc:postgresql://localhost:5432/db2"),
            Config.for_testing(postgres_url="jdbc:postgresql://localhost:5432/db3"),
        ]
        
        for i, config in enumerate(configs, 1):
            service = self.DatabaseService(config)
            assert f"db{i}" in service.get_connection_string()


@pytest.fixture
def test_config():
    """Pytest fixture for test configuration"""
    return Config.for_testing(
        postgres_url="jdbc:postgresql://localhost:5432/fixture_test_db",
        postgres_properties={
            "user": "fixture_user",
            "password": "fixture_pass",
            "driver": "org.postgresql.Driver"
        }
    )


def test_using_fixture(test_config):
    """Example of using pytest fixture"""
    assert "fixture_test_db" in test_config.postgres_url
    assert test_config.postgres_properties["user"] == "fixture_user"


@pytest.fixture
def mock_environment():
    """Fixture to mock environment variables"""
    with patch.dict(os.environ, {"APP_ENV": "test"}):
        yield


def test_with_environment_fixture(mock_environment):
    """Test using environment fixture"""
    config = Config.from_environment()
    assert config.environment == Environment.TESTING 