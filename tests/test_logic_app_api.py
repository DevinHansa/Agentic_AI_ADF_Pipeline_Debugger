import pytest
import json
from dashboard import app, vector_kb_available

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_api_openapi_json(client):
    """Test the OpenAPI JSON endpoint for the Logic App Agent."""
    response = client.get('/api/openapi.json')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['openapi'] == '3.0.1'
    assert 'info' in data
    assert 'paths' in data
    assert '/api/agent_search' in data['paths']

@pytest.mark.skipif(not vector_kb_available, reason="Vector DB not initialized for tests")
def test_api_agent_search(client):
    """Test the Logic App AI Agent Semantic Search tool endpoint."""
    payload = {
        "query": "Operation on target Copy_Data failed: Invalid path"
    }
    response = client.post('/api/agent_search', json=payload)
    assert response.status_code == 200
    
    data = json.loads(response.data)
    assert data['success'] is True
    assert 'top_matches' in data
    assert len(data['top_matches']) > 0
    assert 'title' in data['top_matches'][0]
    assert 'description' in data['top_matches'][0]
    assert 'solution' in data['top_matches'][0]
    assert 'severity' in data['top_matches'][0]
