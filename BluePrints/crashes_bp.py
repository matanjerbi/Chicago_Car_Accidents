from datetime import datetime, timedelta
from time import daylight

from flask import Blueprint, jsonify, request
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from services.read_from_csv import read_csv, safe_int, extract_date

crash_bp = Blueprint('crash', __name__)

client = MongoClient('mongodb://localhost:27017')
db = client['chicago_accidents']
crashes = db['crashes']




@crash_bp.route('/initialize', methods=['POST'])
def initialize_db():
    file_path = '/Users/matanjerbi/Downloads/Traffic_Crashes_-_Crashes - 20k rows.csv'

    try:
        crashes.delete_many({})

        for row in read_csv(file_path):
            injuries = {
                'INJURIES_TOTAL': safe_int(row['INJURIES_TOTAL']),
                'INJURIES_FATAL': safe_int(row['INJURIES_FATAL']),
                'INJURIES_NOT_FATAL': (safe_int(row['INJURIES_TOTAL']) or 0) - (safe_int(row['INJURIES_FATAL']) or 0)
            }
            crash = {
                '_id': row['CRASH_RECORD_ID'],
                'DATE': extract_date(row['CRASH_DATE']),
                'BEAT_OF_OCCURRENCE': safe_int(row['BEAT_OF_OCCURRENCE']),
                'PRIM_CONTRIBUTORY_CAUSE': row['PRIM_CONTRIBUTORY_CAUSE'],
                'INJURIES': injuries
            }
            crashes.insert_one(crash)

        crashes.create_index("BEAT_OF_OCCURRENCE")
        crashes.create_index("DATE")
        crashes.create_index("PRIM_CONTRIBUTORY_CAUSE")

        return jsonify({"message": "Database initialized successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@crash_bp.route('/<beat>', methods=['GET'])
def total_accidents_by_beat(beat):
    try:
        beat_int = int(beat)
        total = db.crashes.count_documents({"BEAT_OF_OCCURRENCE": beat_int})

        return jsonify({
            "beat": beat_int,
            "total_accidents": total
        }), 200
    except ValueError:
        return jsonify({"error": "Invalid beat number"}), 400
    except OperationFailure as e:
        return jsonify({"error": f"Database operation failed: {str(e)}"}), 500


@crash_bp.route("/beat_time/<area>/<date>/time=<period>", methods=["GET"])
def sum_crash_by_area_time(area, date, period):
    try:
        area = int(area)
        time_periods = {
            "day": 1,
            "week": 7,
            "month": 30
        }

        add_date = time_periods.get(period)
        if add_date is None:
            return jsonify({"error": "Invalid time period. Use 'day', 'week', or 'month'."}), 400

        start_date = datetime.strptime(date, '%m-%d-%Y')
        end_date = start_date + timedelta(days=add_date)

        total_crashes = db.crashes.count_documents({
            "BEAT_OF_OCCURRENCE": area,
            "DATE": {"$gte": start_date, "$lt": end_date}
        })

        return jsonify({
            "start_date": start_date,
            "end_date": end_date,
            "area": area,
            "period": period,
            "total_crashes": total_crashes
        }), 200

    except ValueError as ve:
        return jsonify({"error": f"Invalid input: {str(ve)}"}), 400
    except OperationFailure as of:
        return jsonify({"error": f"Database operation failed: {str(of)}"}), 500
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@crash_bp.route('/cause/<beat>', methods=['GET'])
def group_accidents_by_prim_cause(beat):
    try:
        beat = int(beat)
        pipeline = [
            {'$match': {'BEAT_OF_OCCURRENCE': beat}},
            {'$group': {
                'reason': "$PRIM_CONTRIBUTORY_CAUSE",
                'total_accidents': {'$sum': 1}
            }},
            {'$sort': {'total_accidents': -1}}  # מיון לפי מספר התאונות בסדר יורד
        ]

        result = list(db.crashes.aggregate(pipeline))

        return jsonify(result), 200

    except ValueError:
        return jsonify({"error": "Invalid beat number. Must be an integer."}), 400

