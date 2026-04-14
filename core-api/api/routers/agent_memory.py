"""
Agent memory and knowledge base endpoints.
Handles CRUD and vector search for agent memories and workspace institutional knowledge.
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import logging

from api.dependencies import get_current_user_id, get_current_user_jwt
from api.services.agents.memory import (
    store_memory,
    search_memories,
    get_memories,
    update_memory,
    delete_memory,
    access_memory,
    add_knowledge,
    search_knowledge,
    get_knowledge,
    verify_knowledge,
    delete_knowledge,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["agent-memory"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================


class StoreMemoryRequest(BaseModel):
    """Request to store a new memory for an agent."""
    workspace_id: str
    memory_type: str = Field(..., pattern="^(episodic|semantic|procedural|preference)$")
    content: str = Field(..., min_length=1)
    summary: Optional[str] = None
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    tags: Optional[List[str]] = None
    is_shared: bool = False
    decay_factor: float = Field(default=0.95, gt=0, le=1)


class UpdateMemoryRequest(BaseModel):
    """Request to update an existing memory."""
    content: Optional[str] = Field(None, min_length=1)
    summary: Optional[str] = None
    tags: Optional[List[str]] = None
    is_shared: Optional[bool] = None
    relevance_score: Optional[float] = Field(None, ge=0, le=1)


class SearchMemoriesRequest(BaseModel):
    """Request to search agent memories."""
    query: str = Field(..., min_length=1)
    workspace_id: str
    match_count: int = Field(default=10, ge=1, le=100)
    similarity_threshold: float = Field(default=0.5, ge=0, le=1)


class MemoryResponse(BaseModel):
    """A single agent memory."""
    id: str
    agent_id: str
    workspace_id: str
    memory_type: str
    content: str
    summary: Optional[str] = None
    relevance_score: float
    access_count: int
    last_accessed_at: Optional[str] = None
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    tags: List[str]
    is_shared: bool
    decay_factor: float
    created_at: str
    updated_at: str
    expired_at: Optional[str] = None

    class Config:
        extra = "allow"


class MemoryListResponse(BaseModel):
    """Response for listing memories."""
    memories: List[MemoryResponse]
    count: int


class MemorySearchResult(BaseModel):
    """A memory search result with similarity score."""
    id: str
    memory_type: str
    content: str
    summary: Optional[str] = None
    tags: List[str] = []
    relevance_score: float
    similarity: float
    created_at: str

    class Config:
        extra = "allow"


class MemorySearchResponse(BaseModel):
    """Response for memory search."""
    results: List[MemorySearchResult]
    count: int


class AddKnowledgeRequest(BaseModel):
    """Request to add a knowledge base entry."""
    category: str = Field(..., min_length=1, max_length=100)
    key: str = Field(..., min_length=1, max_length=200)
    value: str = Field(..., min_length=1)
    source_agent_id: Optional[str] = None
    confidence: float = Field(default=1.0, ge=0, le=1)


class SearchKnowledgeRequest(BaseModel):
    """Request to search the knowledge base."""
    query: str = Field(..., min_length=1)
    match_count: int = Field(default=10, ge=1, le=100)
    similarity_threshold: float = Field(default=0.5, ge=0, le=1)


class KnowledgeResponse(BaseModel):
    """A single knowledge base entry."""
    id: str
    workspace_id: str
    category: str
    key: str
    value: str
    confidence: float
    source_agent_id: Optional[str] = None
    verified_by: Optional[str] = None
    verified_at: Optional[str] = None
    created_at: str
    updated_at: str

    class Config:
        extra = "allow"


class KnowledgeListResponse(BaseModel):
    """Response for listing knowledge entries."""
    entries: List[KnowledgeResponse]
    count: int


class KnowledgeSearchResult(BaseModel):
    """A knowledge search result with similarity score."""
    id: str
    category: str
    key: str
    value: str
    confidence: float
    similarity: float
    created_at: str

    class Config:
        extra = "allow"


class KnowledgeSearchResponse(BaseModel):
    """Response for knowledge search."""
    results: List[KnowledgeSearchResult]
    count: int


class DeleteResponse(BaseModel):
    """Response for delete operations."""
    status: str


# =============================================================================
# MEMORY ENDPOINTS
# =============================================================================


@router.post("/agents/{agent_id}/memories", response_model=MemoryResponse)
async def store_memory_endpoint(
    agent_id: str,
    request: StoreMemoryRequest,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Store a new memory for an agent."""
    try:
        memory = await store_memory(
            agent_id=agent_id,
            workspace_id=request.workspace_id,
            memory_type=request.memory_type,
            content=request.content,
            user_jwt=user_jwt,
            summary=request.summary,
            source_type=request.source_type,
            source_id=request.source_id,
            tags=request.tags,
            is_shared=request.is_shared,
            decay_factor=request.decay_factor,
        )
        return memory
    except Exception as e:
        logger.error(f"Error storing memory: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/agents/{agent_id}/memories", response_model=MemoryListResponse)
async def list_memories(
    agent_id: str,
    memory_type: Optional[str] = None,
    limit: int = 50,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """List memories for an agent."""
    try:
        memories = await get_memories(agent_id, user_jwt, memory_type=memory_type, limit=limit)
        return {"memories": memories, "count": len(memories)}
    except Exception as e:
        logger.error(f"Error listing memories: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/agents/{agent_id}/memories/search", response_model=MemorySearchResponse)
async def search_memories_endpoint(
    agent_id: str,
    request: SearchMemoriesRequest,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Search agent memories using vector similarity."""
    try:
        results = await search_memories(
            agent_id=agent_id,
            workspace_id=request.workspace_id,
            query=request.query,
            user_jwt=user_jwt,
            match_count=request.match_count,
            similarity_threshold=request.similarity_threshold,
        )
        return {"results": results, "count": len(results)}
    except Exception as e:
        logger.error(f"Error searching memories: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/memories/{memory_id}", response_model=MemoryResponse)
async def update_memory_endpoint(
    memory_id: str,
    request: UpdateMemoryRequest,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Update an existing memory."""
    try:
        updates = request.model_dump(exclude_none=True)
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")
        memory = await update_memory(memory_id, updates, user_jwt)
        return memory
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating memory: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/memories/{memory_id}", response_model=DeleteResponse)
async def delete_memory_endpoint(
    memory_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Delete a memory."""
    try:
        await delete_memory(memory_id, user_jwt)
        return {"status": "deleted"}
    except Exception as e:
        logger.error(f"Error deleting memory: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# =============================================================================
# KNOWLEDGE BASE ENDPOINTS
# =============================================================================


@router.post("/workspaces/{workspace_id}/knowledge", response_model=KnowledgeResponse)
async def add_knowledge_endpoint(
    workspace_id: str,
    request: AddKnowledgeRequest,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Add an entry to the workspace knowledge base."""
    try:
        entry = await add_knowledge(
            workspace_id=workspace_id,
            category=request.category,
            key=request.key,
            value=request.value,
            user_jwt=user_jwt,
            source_agent_id=request.source_agent_id,
            confidence=request.confidence,
        )
        return entry
    except Exception as e:
        logger.error(f"Error adding knowledge: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/workspaces/{workspace_id}/knowledge", response_model=KnowledgeListResponse)
async def list_knowledge(
    workspace_id: str,
    category: Optional[str] = None,
    limit: int = 50,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """List knowledge base entries for a workspace."""
    try:
        entries = await get_knowledge(workspace_id, user_jwt, category=category, limit=limit)
        return {"entries": entries, "count": len(entries)}
    except Exception as e:
        logger.error(f"Error listing knowledge: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/workspaces/{workspace_id}/knowledge/search", response_model=KnowledgeSearchResponse)
async def search_knowledge_endpoint(
    workspace_id: str,
    request: SearchKnowledgeRequest,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Search the workspace knowledge base using vector similarity."""
    try:
        results = await search_knowledge(
            workspace_id=workspace_id,
            query=request.query,
            user_jwt=user_jwt,
            match_count=request.match_count,
            similarity_threshold=request.similarity_threshold,
        )
        return {"results": results, "count": len(results)}
    except Exception as e:
        logger.error(f"Error searching knowledge: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/knowledge/{knowledge_id}/verify", response_model=KnowledgeResponse)
async def verify_knowledge_endpoint(
    knowledge_id: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Mark a knowledge entry as verified by the current user."""
    try:
        entry = await verify_knowledge(knowledge_id, user_id, user_jwt)
        return entry
    except Exception as e:
        logger.error(f"Error verifying knowledge: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/knowledge/{knowledge_id}", response_model=DeleteResponse)
async def delete_knowledge_endpoint(
    knowledge_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Delete a knowledge base entry."""
    try:
        await delete_knowledge(knowledge_id, user_jwt)
        return {"status": "deleted"}
    except Exception as e:
        logger.error(f"Error deleting knowledge: {e}")
        raise HTTPException(status_code=400, detail=str(e))
