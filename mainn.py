from fastapi import FastAPI, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from dotenv import load_dotenv
import csv
import io
import os

# =====================
# DATABASE
# =====================
load_dotenv()

DATABASE_URL = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI(title="Export Appointment API")

# =====================
# EXPORT API
# =====================
@app.get("/export-upcoming-appointments")
def export_upcoming_appointments(db=Depends(get_db)):
    today = datetime(2025, 12, 2)
    next_7_days = today + timedelta(days=7)

    sql = text("""
        SELECT
            DATE(e.starts_on)                    AS appointment_date,
            CONCAT_WS(' ', l.first_name, l.middle_name, l.last_name) AS full_name,
            l.gender                       AS gender,
            l.phone                        AS customer_phone,
            l.email_id                     AS customer_email,
            l.website                      AS customer_website,
            l.fax                          AS customer_fax,
            l.job_title                    AS title
        FROM tabEvent e
        LEFT JOIN `tabEvent Participants` p ON e.name = p.parent 
            AND p.reference_doctype = 'Opportunity'
        LEFT JOIN tabOpportunity o ON p.reference_docname = o.name
        LEFT JOIN tabLead l ON o.party_name = l.name
        WHERE e.starts_on >= :today
        AND   e.starts_on < :next_7_days
        ORDER BY e.starts_on ASC;
    """)

    result = db.execute(
        sql,
        {
            "today": today,
            "next_7_days": next_7_days
        }
    )

    rows = result.mappings().all()

    if not rows:
        return {"message": "Không có lịch hẹn nào trong 7 ngày tới"}

    # =====================
    # STREAM CSV (KHÔNG LƯU FILE)
    # =====================
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=upcoming_appointments.csv"
        }
    )
