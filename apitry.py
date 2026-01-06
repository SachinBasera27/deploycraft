from fastapi import FastAPI, Query, HTTPException
import pandas as pd
from typing import Optional, List

app = FastAPI(title="TRIALDB Dataset API")

# Load the dataset at startup
# Ensure 'TRIALDB.csv' is in the same directory as this script
try:
    df = pd.read_csv('TRIALDB.csv')
    # Clean up column names just in case there are leading/trailing spaces
    df.columns = df.columns.str.strip()
except FileNotFoundError:
    print("Error: TRIALDB.csv not found. Please ensure the file is in the correct directory.")
    df = pd.DataFrame()

@app.get("/")
def read_root():
    return {
        "message": "Welcome to the Institution Data API",
        "total_records": len(df),
        "endpoints": {
            "all_data": "/records",
            "search_by_id": "/records/{insid}",
            "docs": "/docs"
        }
    }

@app.get("/records")
def get_records(
    page: int = Query(1, ge=1), 
    size: int = Query(10, ge=1, le=100),
    insname: Optional[str] = None,
    creddesc: Optional[str] = None
):
    """
    Returns a paginated list of records.
    - page: Page number
    - size: Number of items per page (max 100)
    - insname: Filter by Institution Name (partial match)
    - creddesc: Filter by Credential Description (partial match)
    """
    filtered_df = df.copy()

    if insname:
        filtered_df = filtered_df[filtered_df['INSNAME'].str.contains(insname, case=False, na=False)]
    
    if creddesc:
        filtered_df = filtered_df[filtered_df['CREDDESC'].str.contains(creddesc, case=False, na=False)]

    # Pagination logic
    start = (page - 1) * size
    end = start + size
    
    results = filtered_df.iloc[start:end].to_dict(orient="records")
    
    return {
        "page": page,
        "size": len(results),
        "total_filtered": len(filtered_df),
        "data": results
    }

@app.get("/records/{insid}")
def get_records_by_insid(insid: int):
    """
    Filter records by a specific Institution ID (INSID).
    """
    results = df[df['INSID'] == insid].to_dict(orient="records")
    if not results:
        raise HTTPException(status_code=404, detail=f"No records found for INSID {insid}")
    return {"insid": insid, "count": len(results), "data": results}

@app.get("/stats")
def get_stats():
    """
    Returns unique counts for key categories.
    """
    return {
        "unique_institutions": int(df['INSNAME'].nunique()),
        "unique_credentials": int(df['CREDDESC'].nunique()),
        "credential_levels": df['CREDLEV'].unique().tolist()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)