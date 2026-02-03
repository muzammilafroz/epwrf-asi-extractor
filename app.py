"""
EPWRF Bulk Data Downloader - Web GUI
Flask application for automated bulk downloads from EPWRF India Time Series
"""

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import threading
import os
import time
from datetime import datetime
from automation import EPWRFAutomation

app = Flask(__name__)
app.config['SECRET_KEY'] = 'epwrf-downloader-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global state
automation_thread = None
stop_flag = False
current_automation = None

# Data constants
STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jammu & Kashmir",
    "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra",
    "Manipur", "Meghalaya", "Nagaland", "Orissa", "Punjab", "Rajasthan",
    "Sikkim", "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh",
    "Uttarakhand", "West Bengal", "Andaman & Nicobar Islands", "Chandigarh",
    "Dadra & Nagar Haveli", "Daman & Diu", "Goa, Daman & Diu", "Delhi",
    "Puducherry", "Mizoram", "Ladakh", "Dadra & Nagar Haveli & Daman & Diu",
    "Lakshadweep"
]

VARIABLES = {
    1: "Number of Factories",
    2: "Fixed Capital",
    3: "Working Capital",
    4: "Physical Working Capital",
    5: "Productive Capital",
    6: "Invested Capital",
    7: "Outstanding Loans",
    8: "Fuels Consumed - Total",
    9: "Materials Consumed",
    10: "Total Input",
    11: "Rent Paid",
    12: "Interest Paid",
    13: "Depreciation",
    14: "Products and By-products",
    15: "Value of Gross Output",
    16: "Net Income",
    17: "Profits",
    18: "Net Value Added",
    19: "Gross Value Added",
    20: "Net Fixed Capital Formation",
    21: "Gross Fixed Capital Formation",
    22: "Additions to Total Stock",
    23: "Gross Capital Formation",
    24: "Total Persons Engaged",
    25: "Number of Workers",
    26: "Number of Workers - Directly Employed",
    27: "Number of Workers - Directly Employed - Men",
    28: "Number of Workers - Directly Employed - Women",
    29: "Number of Workers - Employed Through Contractors",
    30: "Employees Other Than Workers",
    31: "Supervisory and Managerial Staff",
    32: "Other Employees",
    33: "Unpaid family members/proprietor etc",
    34: "Number of Mandays - Employees",
    35: "Total Emoluments including Employers' Contribution",
    36: "Total Emoluments",
    37: "Wages and Salaries - Total",
    38: "Wages and Salaries - Workers",
    39: "Wages and Salaries - Supervisory and Managerial Staff",
    40: "Wages and Salaries - Other Employees",
    41: "Bonus to All Staff",
    42: "PF and Other Benefits",
    43: "Number of Factories in Operation",
    44: "Gross Value of Addition to Fixed Capital",
    45: "Gross Value of Plant and Machinery",
    46: "Rent Received",
    47: "Interest Received",
    48: "Addition in Stock of Materials, Fuels, etc.",
    49: "Addition in Stock of Semi-Finished Goods",
    50: "Addition in Stock of Finished Goods",
    51: "Number of Workers - Directly Employed - Children",
    52: "Number of Employees",
    53: "Fuels Consumed - Coal (Quantity)",
    54: "Fuels Consumed - Coal (Value)",
    55: "Fuels Consumed - Electricity Purchased (Quantity)",
    56: "Fuels Consumed - Electricity (Value)",
    57: "Fuels Consumed - Petroleum Products (Value)",
    58: "Fuels Consumed - Other Fuel (Value)"
}

NIC_CODES = {
    "168": "14 - Manufacture of Wearing Apparel",
    "166": "13 - Manufacture of Textiles",
    "170": "15 - Manufacture of Leather",
    "172": "16 - Manufacture of Wood Products",
    "174": "17 - Manufacture of Paper Products",
    "176": "18 - Printing and Reproduction",
    "178": "19 - Manufacture of Coke and Refined Petroleum",
    "180": "20 - Manufacture of Chemicals",
    "182": "21 - Manufacture of Pharmaceuticals",
    "184": "22 - Manufacture of Rubber and Plastics",
    "186": "23 - Manufacture of Non-metallic Mineral Products",
    "188": "24 - Manufacture of Basic Metals",
    "190": "25 - Manufacture of Fabricated Metal Products",
    "192": "26 - Manufacture of Computer and Electronics",
    "194": "27 - Manufacture of Electrical Equipment",
    "196": "28 - Manufacture of Machinery",
    "198": "29 - Manufacture of Motor Vehicles",
    "200": "30 - Manufacture of Other Transport Equipment",
    "202": "31 - Manufacture of Furniture",
    "204": "32 - Other Manufacturing",
}

YEARS = [f"{y}-{y+1}" for y in range(1979, 2024)]


@app.route('/')
def index():
    return render_template('index.html', 
                         states=STATES, 
                         variables=VARIABLES, 
                         nic_codes=NIC_CODES,
                         years=YEARS)


@app.route('/api/config')
def get_config():
    return jsonify({
        'states': STATES,
        'variables': VARIABLES,
        'nic_codes': NIC_CODES,
        'years': YEARS
    })


@socketio.on('connect')
def handle_connect():
    emit('status', {'message': 'Connected to server', 'type': 'info'})


@socketio.on('start_download')
def handle_start_download(data):
    global automation_thread, stop_flag, current_automation
    
    if automation_thread and automation_thread.is_alive():
        emit('status', {'message': 'Download already in progress', 'type': 'warning'})
        return
    
    stop_flag = False
    
    # Extract parameters
    nic_codes = data.get('nic_codes', ['168'])
    states = data.get('states', STATES)
    variables = data.get('variables', list(VARIABLES.keys()))
    start_year = data.get('start_year', '1979-1980')
    end_year = data.get('end_year', '2023-2024')
    output_folder = data.get('output_folder', 'downloads')
    
    def progress_callback(msg, msg_type='info', progress=None):
        socketio.emit('status', {'message': msg, 'type': msg_type})
        if progress is not None:
            socketio.emit('progress', progress)
    
    def run_automation():
        global stop_flag, current_automation
        try:
            current_automation = EPWRFAutomation(
                output_folder=output_folder,
                callback=progress_callback
            )
            current_automation.run(
                nic_codes=nic_codes,
                states=states,
                variables=variables,
                start_year=start_year,
                end_year=end_year,
                stop_flag=lambda: stop_flag
            )
        except Exception as e:
            progress_callback(f'Error: {str(e)}', 'error')
        finally:
            current_automation = None
            socketio.emit('download_complete')
    
    automation_thread = threading.Thread(target=run_automation)
    automation_thread.start()
    emit('status', {'message': 'Download started...', 'type': 'success'})


@socketio.on('stop_download')
def handle_stop_download():
    global stop_flag
    stop_flag = True
    emit('status', {'message': 'Stopping download...', 'type': 'warning'})


if __name__ == '__main__':
    os.makedirs('downloads', exist_ok=True)
    print("=" * 50)
    print("EPWRF Bulk Downloader")
    print("Open http://localhost:5000 in your browser")
    print("=" * 50)
    socketio.run(app, debug=True, port=5000)
