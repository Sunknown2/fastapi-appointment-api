from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import date, datetime, timedelta
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

Base_SQL = """
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
    """

# =====================
# EXPORT API
# =====================
@app.get("/this-week-appointments")
def export_upcoming_appointments(db=Depends(get_db)):
    today = date.today()
    #next_7_days = today + timedelta(days=7)
    weekday = today.weekday()
    end_of_week = today + timedelta(days=(6 - weekday))

    sql = text(Base_SQL + """  
        WHERE e.starts_on >= :today
        AND   e.starts_on < :end_of_week
        ORDER BY e.starts_on ASC;
    """)

    result = db.execute(
        sql,
        {
            "today": today,
            "end_of_week": end_of_week
        }
    )

    rows = result.mappings().all()

    if not rows:
        return {"message": "Không có lịch hẹn nào trong tuần này"}

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

@app.get("/last-7-days-appointment")
def appointments_last_7_days(db=Depends(get_db)):
    today = date.today()
    start_date = today - timedelta(days=7)

    sql = text(Base_SQL + """
        WHERE e.starts_on >= :start_date
          AND e.starts_on <= :today
        ORDER BY e.starts_on DESC
    """)

    result = db.execute(
        sql,
        {
            "today": today,
            "start_date": start_date
        }
    )

    rows = result.mappings().all()

    if not rows:
        return {"message": "Không có lịch hẹn nào trong 7 ngày gần nhất"}

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

@app.get("/check-appointment")
def appointments_until(
    to_date: date = Query(..., description="YYYY-MM-DD"),
    db=Depends(get_db)
):
    today = date.today()

    if to_date < today:
        raise HTTPException(
            status_code=400,
            detail="Ngày nhập phải >= hôm nay"
        )

    sql = text(Base_SQL + """
        WHERE e.starts_on >= :today
          AND e.starts_on <= :to_date
        ORDER BY e.starts_on
    """)
    result = db.execute(
        sql,
        {
            "today": today,
            "to_date": to_date
        }
    )

    rows = result.mappings().all()

    if not rows:
        return {"message": f"Không có lịch hẹn nào từ hôm nay đến ngày {to_date}"}

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