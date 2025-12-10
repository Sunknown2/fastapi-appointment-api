from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from datetime import date, timedelta
import pandas as pd

from database import get_db
from models import Appointment, Customer, Staff

app = FastAPI(title="Appointment Export API")

@app.get("/export-upcoming-appointments")
def export_upcoming_appointments(db: Session = Depends(get_db)):
    today = date.today()
    next_7_days = today + timedelta(days=7)

    results = (
        db.query(
            Appointment.id,
            Appointment.appointment_date,
            Appointment.appointment_time,
            Appointment.status,
            Appointment.note,

            Customer.name.label("customer_name"),
            Customer.phone.label("customer_phone"),
            Customer.email.label("customer_email"),

            Staff.name.label("staff_name"),
            Staff.department.label("staff_department"),
        )
        .join(Customer, Appointment.customer_id == Customer.id)
        .join(Staff, Appointment.staff_id == Staff.id)
        .filter(
            Appointment.appointment_date >= today,
            Appointment.appointment_date <= next_7_days
        )
        .order_by(Appointment.appointment_date, Appointment.appointment_time)
        .all()
    )

    data = [dict(row._mapping) for row in results]

    df = pd.DataFrame(data)

    file_path = "upcoming_appointments.csv"
    df.to_csv(file_path, index=False, encoding="utf-8-sig")

    return FileResponse(
        path=file_path,
        filename="upcoming_appointments.csv",
        media_type="text/csv"
    )
