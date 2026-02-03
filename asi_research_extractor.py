#!/usr/bin/env python3
"""
ASI Research Data Extractor
============================
Focused extraction for manufacturing industry research (NIC 10-33).

Research Focus:
- GVA, Output, Productivity
- Employment (workers, intensity, gender)
- Capital, Wages, Input costs

Subsections to Extract:
- Subsection 2: 2-Digit Industry Groups (All India)
- Subsection 7: 2-Digit Industry by States (Regional analysis)
- Subsection 10: Employment Characteristics (Detailed employment)

Output Structure:
./asi_research_data/
├── raw/
│   ├── sub02_allIndia_2digit/
│   │   ├── epwrf_concorded/
│   │   │   ├── sub02_var01_gross_value_added_batch01_nic10-12.xls
│   │   │   ├── sub02_var01_gross_value_added_batch02_nic13-15.xls
│   │   │   └── ...
│   ├── sub07_states_2digit/
│   ├── sub10_employment/
│   └── sub11_employment_states/
├── logs/
├── extraction_log.json      # Full metadata for ETL pipeline
├── manifest.json            # Summary of extraction
└── database/
    └── asi_research.db

Usage:
------
# Extract Subsection 2 (All India 2-digit) - START HERE
python asi_research_extractor.py --subsection 2 --output ./asi_research_data

# Extract Subsection 7 (State-wise 2-digit)
python asi_research_extractor.py --subsection 7 --output ./asi_research_data

# Extract Subsection 10 (Employment characteristics)
python asi_research_extractor.py --subsection 10 --output ./asi_research_data

# Extract all research-relevant subsections
python asi_research_extractor.py --all --output ./asi_research_data

# Test mode (limited extraction)
python asi_research_extractor.py --subsection 2 --test --output ./asi_research_data
"""

import os
import re
import json
import time
import sqlite3
import shutil
import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_URL = "https://epwrfits.in"
INDEX_URL = f"{BASE_URL}/index.aspx"

# Research-relevant subsections
# NOTE: We extract ONLY EPWRF Concorded Series (harmonized across years)
# This provides consistent long-term data for trend analysis
RESEARCH_SUBSECTIONS = {
    2: {
        "name": "2-Digit Industry Groups (All India)",
        "folder": "sub02_allIndia_2digit",
        "short_code": "sub02",
        "radio_id": "rad_grid_2",
        "has_series": True,
        "has_area": False,
        "has_states": False,
        "data_type": "gva_output_productivity",
        "description": "Principal characteristics by 2-digit NIC industry - All India aggregates"
    },
    7: {
        "name": "2-Digit Industry by States",
        "folder": "sub07_states_2digit",
        "short_code": "sub07",
        "radio_id": "rad_grid_7",
        "has_series": True,
        "has_area": False,
        "has_states": True,
        "data_type": "gva_output_by_state",
        "description": "2-digit industry data disaggregated by states"
    },
    10: {
        "name": "Employment Characteristics",
        "folder": "sub10_employment",
        "short_code": "sub10",
        "radio_id": "rad_grid_10",
        "has_series": True,
        "has_area": False,
        "has_states": False,
        "data_type": "employment",
        "description": "Detailed employment metrics by industry"
    },
    11: {
        "name": "Employment by States",
        "folder": "sub11_employment_states",
        "short_code": "sub11",
        "radio_id": "rad_grid_11",
        "has_series": True,
        "has_area": False,
        "has_states": True,
        "data_type": "employment_by_state",
        "description": "Employment metrics disaggregated by states"
    }
}

# Key variables for research - COMPREHENSIVE LIST
# Organized by research area for manufacturing analysis
PRIORITY_VARIABLES = [
    # ===========================================
    # STRUCTURE & SCALE
    # ===========================================
    "Number of Factories",
    "Number of Open Factories",
    
    # ===========================================
    # OUTPUT & VALUE ADDED (GVA)
    # ===========================================
    "Gross Value Added",
    "Net Value Added",
    "Value of Gross Output",
    "Products and By-products",
    "Total Output",
    "Value of Output",
    "Value Added",
    
    # ===========================================
    # EMPLOYMENT - TOTAL
    # ===========================================
    "Number of Workers",
    "Number of Employees",  
    "Total Persons Engaged",
    "Average Daily Persons Engaged",
    "Average Number of Persons Engaged",
    
    # ===========================================
    # EMPLOYMENT - GENDER DISAGGREGATED
    # ===========================================
    "Number of Workers - Male",
    "Number of Workers - Female",
    "Number of Employees - Male", 
    "Number of Employees - Female",
    "Workers - Male",
    "Workers - Female",
    "Employees - Male",
    "Employees - Female",
    "Male Workers",
    "Female Workers",
    "Male Employees",
    "Female Employees",
    
    # ===========================================
    # EMPLOYMENT - MAN HOURS & MAN DAYS
    # ===========================================
    "Number of Man Hours",
    "Number of Man Hours - Workers",
    "Number of Man Hours - Total",
    "Number of Mandays - Workers",
    "Number of Mandays - Employees",
    "Number of Mandays - Total",
    "Mandays Worked",
    "Man Hours Worked",
    
    # ===========================================
    # WAGES & COMPENSATION
    # ===========================================
    "Wages and Salaries - Workers",
    "Wages and Salaries - Total", 
    "Wages to Workers",
    "Salaries to Employees",
    "Total Emoluments",
    "Emoluments to Workers",
    "Emoluments to Persons",
    "PF and Other Benefits",
    "Provident Fund",
    "Bonus",
    "Contribution to PF",
    "Workmen's Compensation",
    
    # ===========================================
    # CAPITAL
    # ===========================================
    "Fixed Capital",
    "Working Capital",
    "Productive Capital",
    "Invested Capital",
    "Physical Working Capital",
    "Outstanding Loans",
    "Book Value of Fixed Assets",
    "Net Fixed Assets",
    
    # ===========================================
    # CAPITAL FORMATION
    # ===========================================
    "Gross Capital Formation",
    "Net Capital Formation",
    "Gross Fixed Capital Formation",
    "Net Fixed Capital Formation",
    "Addition to Fixed Assets",
    "Addition to Stock",
    
    # ===========================================
    # INPUTS
    # ===========================================
    "Total Input",
    "Materials Consumed",
    "Fuels Consumed",
    "Fuels Consumed - Total",
    "Total Inputs",
    "Cost of Materials",
    "Cost of Fuels",
    
    # ===========================================
    # COSTS & EXPENSES
    # ===========================================
    "Depreciation",
    "Rent Paid",
    "Interest Paid",
    "Rent Paid for Plant and Machinery",
    "Rent Paid for Land and Building",
    "Other Expenses",
    
    # ===========================================
    # PROFITS & INCOME
    # ===========================================
    "Profits",
    "Net Income",
    "Net Surplus",
    "Gross Surplus",
]

# Element IDs - Different subsections may use different IDs
# We try multiple patterns for each element type
ELEMENT_IDS = {
    'series': 'ctl00_ContentPlaceHolder1_drpSeries',
    'group_type': 'ctl00_ContentPlaceHolder1_drpType',
    'start_year': 'ctl00_ContentPlaceHolder1_drpStartYear',
    'end_year': 'ctl00_ContentPlaceHolder1_drpEndYear',
    'btn_submit': 'ctl00_ContentPlaceHolder1_btnsubmit',
    'btn_back': 'ctl00_ContentPlaceHolder1_btnback',
    'excel_btn': 'ctl00_ContentPlaceHolder1_ibtnexcel',
}

# Multiple patterns for elements that vary by subsection
VARIABLE_INPUT_IDS = [
    'ctl00_ContentPlaceHolder1_cboVariables_Input',
    'ctl00_ContentPlaceHolder1_cbkSubVariables_ob_CbocbkSubVariablesTB',
    'ctl00_ContentPlaceHolder1_cboType_Input',
]

VARIABLE_CONTAINER_IDS = [
    'ctl00_ContentPlaceHolder1_cboVariables_DropDown',
    'ctl00_ContentPlaceHolder1_cbkSubVariables_ob_CbocbkSubVariablesItemsContainer',
    'ctl00_ContentPlaceHolder1_cboType_DropDown',
]

STATE_INPUT_IDS = [
    'ctl00_ContentPlaceHolder1_cboState_Input',
    'ctl00_ContentPlaceHolder1_drpstate_ob_CbodrpstateTB',
]

STATE_CONTAINER_IDS = [
    'ctl00_ContentPlaceHolder1_cboState_DropDown',
    'ctl00_ContentPlaceHolder1_drpstate_ob_CbodrpstateItemsContainer',
]


# ============================================================================
# LOGGER
# ============================================================================

class Logger:
    """Simple colored logger"""
    
    COLORS = {
        'INFO': '\033[97m', 'SUCCESS': '\033[92m', 'WARNING': '\033[93m',
        'ERROR': '\033[91m', 'ACTION': '\033[94m', 'DATA': '\033[95m',
        'STEP': '\033[96m', 'DEBUG': '\033[90m'
    }
    RESET = '\033[0m'
    
    def __init__(self, log_dir: Path):
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = log_dir / f"extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.file = open(self.log_file, 'a', encoding='utf-8')
    
    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        color = self.COLORS.get(level, self.RESET)
        print(f"{color}[{ts}] [{level:7}] {msg}{self.RESET}")
        self.file.write(f"[{ts}] [{level:7}] {msg}\n")
        self.file.flush()
    
    def info(self, msg): self._log('INFO', msg)
    def success(self, msg): self._log('SUCCESS', msg)
    def warning(self, msg): self._log('WARNING', msg)
    def error(self, msg): self._log('ERROR', msg)
    def action(self, msg): self._log('ACTION', msg)
    def data(self, msg): self._log('DATA', msg)
    def step(self, msg): self._log('STEP', msg)
    def debug(self, msg): self._log('DEBUG', msg)
    
    def close(self):
        self.file.close()


# ============================================================================
# BROWSER
# ============================================================================

class Browser:
    """Selenium browser automation"""
    
    def __init__(self, download_dir: Path, logger: Logger, headless: bool = False):
        self.download_dir = download_dir
        self.logger = logger
        self.driver = None
        self._setup(headless)
    
    def _setup(self, headless: bool):
        options = Options()
        if headless:
            options.add_argument('--headless')
        
        prefs = {
            'download.default_directory': str(self.download_dir.absolute()),
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
        }
        options.add_experimental_option('prefs', prefs)
        options.add_argument('--window-size=1400,900')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 30)
        self.logger.success("Browser started")
    
    def get_page_type(self) -> str:
        url = self.driver.current_url.lower()
        if 'treeview' in url: return 'treeview'
        elif 'displaydata' in url: return 'display'
        elif 'typesofasi' in url: return 'selector'
        elif 'index' in url: return 'index'
        elif 'error' in url: return 'error'
        return 'unknown'
    
    def wait_for_page(self, timeout: int = 30):
        time.sleep(1)
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except:
            pass
        time.sleep(2)
    
    def wait_for_display_page(self, timeout: int = 180) -> bool:
        """Wait up to 3 minutes for display page"""
        self.logger.action(f"Waiting for display page (timeout: {timeout}s)...")
        start = time.time()
        
        while time.time() - start < timeout:
            page_type = self.get_page_type()
            if page_type == 'display':
                self.logger.success(f"Display page loaded in {int(time.time()-start)}s")
                time.sleep(2)
                return True
            if page_type == 'error':
                self.logger.error("Error page!")
                return False
            
            elapsed = int(time.time() - start)
            if elapsed % 20 == 0 and elapsed > 0:
                self.logger.info(f"Still waiting... {elapsed}s")
            time.sleep(3)
        
        self.logger.error(f"Timeout after {timeout}s")
        return False
    
    def navigate_to_asi(self) -> bool:
        """Navigate to ASI selector via index"""
        self.logger.action("Navigating to ASI module...")
        self.driver.get(INDEX_URL)
        self.wait_for_page()
        
        try:
            link = self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//a[contains(@href, 'TypesOfASI')]")))
            self.driver.execute_script("arguments[0].click();", link)
            time.sleep(3)
            self.wait_for_page()
            return 'typesofasi' in self.driver.current_url.lower()
        except Exception as e:
            self.logger.error(f"Navigation failed: {e}")
            return False
    
    def select_subsection(self, radio_id: str) -> bool:
        """Select subsection and go to treeview"""
        try:
            radio = self.wait.until(EC.presence_of_element_located((By.ID, radio_id)))
            self.driver.execute_script("arguments[0].scrollIntoView(true);", radio)
            time.sleep(0.5)
            radio.click()
            time.sleep(1)
            
            submit = self.driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_btnsubmit')
            submit.click()
            self.wait_for_page()
            
            return self.get_page_type() == 'treeview'
        except Exception as e:
            self.logger.error(f"Subsection selection failed: {e}")
            return False
    
    def get_dropdown_options(self, element_id: str) -> List[Dict]:
        """Get dropdown options"""
        options = []
        try:
            elem = self.driver.find_element(By.ID, element_id)
            if elem.is_displayed():
                select = Select(elem)
                for opt in select.options:
                    text = opt.text.strip()
                    if text and 'Select' not in text:
                        options.append({'value': opt.get_attribute('value'), 'text': text})
        except:
            pass
        return options
    
    def select_dropdown(self, element_id: str, value: str = None, index: int = None) -> bool:
        """Select dropdown value"""
        try:
            elem = self.wait.until(EC.presence_of_element_located((By.ID, element_id)))
            select = Select(elem)
            
            if value:
                for opt in select.options:
                    if value in opt.text or opt.get_attribute('value') == value:
                        select.select_by_visible_text(opt.text)
                        time.sleep(0.5)
                        return True
            elif index is not None:
                select.select_by_index(index)
                time.sleep(0.5)
                return True
        except:
            pass
        return False
    
    def expand_tree(self) -> int:
        """Expand all tree nodes"""
        expanded = 0
        for _ in range(100):
            try:
                toggles = self.driver.find_elements(By.CSS_SELECTOR, "img[src*='plus']")
                unexpanded = [t for t in toggles if t.is_displayed() and 'minus' not in (t.get_attribute('src') or '')]
                
                if not unexpanded:
                    break
                
                for t in unexpanded[:1]:
                    t.click()
                    time.sleep(0.5)
                    expanded += 1
            except:
                break
        return expanded
    
    def find_epwrf_node(self):
        """Find the EPWRF Concorded Series tree node element"""
        # Look for nodes containing "EPWRF" or "Concorded"
        search_patterns = [
            "//span[contains(text(), 'EPWRF')]",
            "//span[contains(text(), 'Concorded')]",
            "//a[contains(text(), 'EPWRF')]",
            "//a[contains(text(), 'Concorded')]",
            "//*[contains(text(), \"EPWRF's Concorded\")]",
            "//td[contains(., 'EPWRF')]//ancestor::tr[1]",
        ]
        
        for pattern in search_patterns:
            try:
                nodes = self.driver.find_elements(By.XPATH, pattern)
                for node in nodes:
                    text = node.text.strip()
                    if 'EPWRF' in text or 'Concorded' in text:
                        self.logger.info(f"Found EPWRF node: {text[:60]}...")
                        return node
            except:
                continue
        return None
    
    def expand_epwrf_node_only(self) -> bool:
        """Expand ONLY the EPWRF Concorded Series node"""
        self.logger.action("Looking for EPWRF Concorded Series node...")
        
        # First, find the EPWRF node text
        epwrf_node = self.find_epwrf_node()
        if not epwrf_node:
            self.logger.warning("EPWRF node not found, trying alternate method...")
            # Try finding by looking at all tree text items
            try:
                all_spans = self.driver.find_elements(By.CSS_SELECTOR, "span.ob_t2")
                for span in all_spans:
                    text = span.text.strip()
                    if 'EPWRF' in text or 'Concorded' in text:
                        self.logger.info(f"Found via alternate: {text[:60]}...")
                        epwrf_node = span
                        break
            except:
                pass
        
        if not epwrf_node:
            self.logger.error("Could not find EPWRF Concorded Series node!")
            return False
        
        # Find the expand icon (plus) for this node
        # Usually the expand icon is in a nearby td/tr element
        try:
            # Get the parent row/container
            parent = epwrf_node.find_element(By.XPATH, "./ancestor::tr[1]")
            
            # Look for plus icon in this row
            plus_icons = parent.find_elements(By.XPATH, ".//img[contains(@src, 'plus')]")
            
            if not plus_icons:
                # Try looking at previous sibling td
                plus_icons = parent.find_elements(By.XPATH, "./preceding-sibling::*//img[contains(@src, 'plus')]")
            
            if not plus_icons:
                # Broader search - find plus icon near the EPWRF text
                plus_icons = self.driver.find_elements(By.XPATH, 
                    "//tr[.//span[contains(text(), 'EPWRF')]]//img[contains(@src, 'plus')]")
            
            if plus_icons:
                self.logger.info(f"Found {len(plus_icons)} expand icon(s) for EPWRF node")
                plus_icons[0].click()
                time.sleep(1)
                self.logger.success("Expanded EPWRF node!")
                return True
            else:
                # Check if already expanded (minus icon)
                minus_icons = parent.find_elements(By.XPATH, ".//img[contains(@src, 'minus')]")
                if minus_icons:
                    self.logger.info("EPWRF node already expanded")
                    return True
                self.logger.warning("No expand icon found for EPWRF node")
                return False
                
        except Exception as e:
            self.logger.error(f"Error expanding EPWRF node: {e}")
            return False
    
    def get_epwrf_checkboxes(self) -> List[str]:
        """Get ONLY the checkboxes under EPWRF Concorded Series node"""
        cb_ids = []
        
        self.logger.info("Finding checkboxes under EPWRF node...")
        
        # Method 1: Checkbox IDs contain "EPWRF Concorded Series" in the ID
        # Pattern: chk_True_EPWRF Concorded Series_XXX_description
        try:
            all_cbs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
            for cb in all_cbs:
                if cb.is_displayed():
                    cb_id = cb.get_attribute('id') or ''
                    # Check if this is an EPWRF checkbox by ID pattern
                    if 'EPWRF' in cb_id or 'Concorded' in cb_id:
                        if cb_id not in cb_ids:
                            cb_ids.append(cb_id)
        except Exception as e:
            self.logger.warning(f"Method 1 (ID pattern) failed: {e}")
        
        if cb_ids:
            self.logger.info(f"Found {len(cb_ids)} EPWRF checkboxes by ID pattern")
            return cb_ids
        
        # Method 2: Find checkboxes with NIC codes (2-digit codes like 10, 11, 12, etc.)
        try:
            all_cbs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
            for cb in all_cbs:
                if cb.is_displayed():
                    cb_id = cb.get_attribute('id')
                    if cb_id:
                        # Find associated text - look for NIC code format
                        try:
                            parent_tr = cb.find_element(By.XPATH, "./ancestor::tr[1]")
                            spans = parent_tr.find_elements(By.CSS_SELECTOR, "span")
                            
                            for span in spans:
                                text = span.text.strip()
                                # Check if it's an NIC code (starts with 2 digits)
                                if text and len(text) > 2 and text[0:2].isdigit():
                                    if cb_id not in cb_ids:
                                        cb_ids.append(cb_id)
                                    break
                        except:
                            pass
        except Exception as e:
            self.logger.warning(f"Method 2 (NIC code) failed: {e}")
        
        self.logger.info(f"Found {len(cb_ids)} EPWRF industry checkboxes")
        return cb_ids
    
    def get_manufacturing_checkboxes(self) -> List[str]:
        """Get only manufacturing industry checkboxes (NIC codes 10-33)"""
        all_epwrf = self.get_epwrf_checkboxes()
        mfg_cbs = []
        
        for cb_id in all_epwrf:
            try:
                # Try to extract NIC code from checkbox ID or associated text
                elem = self.driver.find_element(By.ID, cb_id)
                parent_tr = elem.find_element(By.XPATH, "./ancestor::tr[1]")
                spans = parent_tr.find_elements(By.CSS_SELECTOR, "span")
                
                for span in spans:
                    text = span.text.strip()
                    if text and len(text) >= 2 and text[0:2].isdigit():
                        nic_code = int(text[:2])
                        if 10 <= nic_code <= 33:  # Manufacturing range
                            mfg_cbs.append(cb_id)
                        break
            except:
                # If we can't parse, check ID for common manufacturing keywords
                if any(kw in cb_id.lower() for kw in ['manufactur', 'food', 'textile', 'chemical', 'metal', 'machine']):
                    mfg_cbs.append(cb_id)
        
        self.logger.info(f"Filtered to {len(mfg_cbs)} manufacturing checkboxes (NIC 10-33)")
        return mfg_cbs
    
    def get_tree_checkboxes(self) -> List[str]:
        """Get all tree checkbox IDs"""
        cb_ids = []
        selectors = [
            "input.chkbox[type='checkbox']",
            "input[type='checkbox'][id^='chk_']",
            "input[type='checkbox'][id*='ob_t']",
        ]
        seen = set()
        
        for sel in selectors:
            try:
                for cb in self.driver.find_elements(By.CSS_SELECTOR, sel):
                    cb_id = cb.get_attribute('id')
                    if cb_id and cb_id not in seen and cb.is_displayed():
                        seen.add(cb_id)
                        cb_ids.append(cb_id)
            except:
                pass
        return cb_ids
    
    def select_checkboxes(self, cb_ids: List[str]) -> int:
        """Select specific checkboxes"""
        selected = 0
        for cb_id in cb_ids:
            try:
                elem = self.driver.find_element(By.ID, cb_id)
                if not elem.is_selected():
                    self.driver.execute_script("arguments[0].click();", elem)
                    selected += 1
                    time.sleep(0.1)
            except:
                pass
        return selected
    
    def deselect_all_checkboxes(self):
        """Deselect all tree checkboxes"""
        for cb_id in self.get_tree_checkboxes():
            try:
                elem = self.driver.find_element(By.ID, cb_id)
                if elem.is_selected():
                    self.driver.execute_script("arguments[0].click();", elem)
            except:
                pass
    
    def select_all_states(self) -> int:
        """Select all states from obout combo - tries multiple possible element IDs"""
        selected = 0
        
        # Try multiple possible state input IDs
        inp = None
        for inp_id in STATE_INPUT_IDS:
            try:
                inp = self.driver.find_element(By.ID, inp_id)
                if inp.is_displayed():
                    self.logger.debug(f"Found state input: {inp_id}")
                    break
            except:
                continue
        
        if not inp:
            self.logger.warning("Could not find state input element")
            return 0
        
        try:
            # Scroll into view and click
            self.driver.execute_script("arguments[0].scrollIntoView(true);", inp)
            time.sleep(0.3)
            self.driver.execute_script("arguments[0].click();", inp)
            time.sleep(1)
            
            # Find container
            container = None
            for cont_id in STATE_CONTAINER_IDS:
                try:
                    container = self.driver.find_element(By.ID, cont_id)
                    if container.is_displayed():
                        self.logger.debug(f"Found state container: {cont_id}")
                        break
                except:
                    continue
            
            if not container:
                self.logger.warning("Could not find state container")
                return 0
            
            # Select all checkboxes in container
            for cb in container.find_elements(By.CSS_SELECTOR, "input[type='checkbox']"):
                if not cb.is_selected():
                    self.driver.execute_script("arguments[0].click();", cb)
                    selected += 1
                    time.sleep(0.03)
            
            # Close dropdown
            self.driver.execute_script("arguments[0].click();", inp)
            time.sleep(0.5)
            
            self.logger.info(f"Selected {selected} states")
        except Exception as e:
            self.logger.debug(f"State selection error: {e}")
        return selected
    
    def get_variables(self) -> List[str]:
        """Get available variable names - filters out category headers"""
        variables = []
        
        # Headers/categories to skip (not actual variables)
        HEADER_PATTERNS = [
            "Principal Characteristics",
            "Industry Group",
            "All India",
            "State Wise",
            "By Industry",
            "Select All",
            "Select",
        ]
        
        # Try multiple possible variable input/container IDs
        inp = None
        container = None
        
        for inp_id in VARIABLE_INPUT_IDS:
            try:
                inp = self.driver.find_element(By.ID, inp_id)
                if inp.is_displayed():
                    self.logger.debug(f"Found variable input: {inp_id}")
                    break
            except:
                continue
        
        if not inp:
            self.logger.warning("Could not find variable input element")
            return variables
        
        try:
            # Close any open state dropdown first (click elsewhere)
            self.driver.execute_script("arguments[0].scrollIntoView(true);", inp)
            time.sleep(0.3)
            inp.click()
            time.sleep(0.8)
            
            # Find container
            for cont_id in VARIABLE_CONTAINER_IDS:
                try:
                    container = self.driver.find_element(By.ID, cont_id)
                    if container.is_displayed():
                        self.logger.debug(f"Found variable container: {cont_id}")
                        break
                except:
                    continue
            
            if container:
                for item in container.find_elements(By.CSS_SELECTOR, "ul li b"):
                    text = item.text.strip()
                    if text:
                        # Skip if it matches a header pattern
                        is_header = any(pattern.lower() in text.lower() for pattern in HEADER_PATTERNS)
                        if not is_header:
                            variables.append(text)
                        else:
                            self.logger.debug(f"Skipping header: {text[:50]}...")
            
            inp.click()  # Close dropdown
            time.sleep(0.3)
        except Exception as e:
            self.logger.debug(f"get_variables error: {e}")
        
        self.logger.info(f"Found {len(variables)} selectable variables")
        return variables
    
    def select_variable(self, var_name: str) -> bool:
        """Select a variable from obout combo - skips headers"""
        # Headers/categories to skip
        HEADER_PATTERNS = [
            "Principal Characteristics",
            "Industry Group",
            "All India",
            "State Wise",
            "By Industry",
            "Select All",
        ]
        
        # Try multiple possible variable input/container IDs
        inp = None
        container = None
        
        for inp_id in VARIABLE_INPUT_IDS:
            try:
                inp = self.driver.find_element(By.ID, inp_id)
                if inp.is_displayed():
                    break
            except:
                continue
        
        if not inp:
            self.logger.warning("Could not find variable input element")
            return False
        
        try:
            # Scroll into view and click
            self.driver.execute_script("arguments[0].scrollIntoView(true);", inp)
            time.sleep(0.3)
            self.driver.execute_script("arguments[0].click();", inp)
            time.sleep(0.8)
            
            # Find container
            for cont_id in VARIABLE_CONTAINER_IDS:
                try:
                    container = self.driver.find_element(By.ID, cont_id)
                    if container.is_displayed():
                        break
                except:
                    continue
            
            if not container:
                self.logger.warning("Could not find variable container")
                return False
            
            for item in container.find_elements(By.CSS_SELECTOR, "ul li"):
                try:
                    text_elem = item.find_element(By.TAG_NAME, 'b')
                    text = text_elem.text.strip()
                    
                    # Skip headers
                    if any(pattern.lower() in text.lower() for pattern in HEADER_PATTERNS):
                        continue
                    
                    # Match variable name
                    if var_name.lower() in text.lower() or text.lower() in var_name.lower():
                        self.logger.info(f"Selecting variable: {text[:50]}...")
                        self.driver.execute_script("arguments[0].click();", item)
                        time.sleep(0.3)
                        return True
                except:
                    pass
            
            inp.click()  # Close dropdown
            self.logger.warning(f"Could not find variable: {var_name}")
        except Exception as e:
            self.logger.debug(f"select_variable error: {e}")
        return False
    
    def click_submit(self) -> bool:
        """Click submit button to go to display page"""
        try:
            # Try multiple possible submit button IDs
            btn_ids = [
                ELEMENT_IDS['btn_submit'],
                'ctl00_ContentPlaceHolder1_btnsubmit',
                'ctl00_ContentPlaceHolder1_Button1',
                'btnsubmit'
            ]
            
            for btn_id in btn_ids:
                try:
                    btn = self.driver.find_element(By.ID, btn_id)
                    if btn.is_displayed():
                        self.logger.action(f"Clicking submit button: {btn_id}")
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                        time.sleep(0.3)
                        self.driver.execute_script("arguments[0].click();", btn)
                        self.logger.success("Submit clicked!")
                        return True
                except:
                    continue
            
            # Try by text/value
            btns = self.driver.find_elements(By.CSS_SELECTOR, "input[type='submit'], button")
            for btn in btns:
                text = btn.get_attribute('value') or btn.text or ''
                if 'submit' in text.lower() or 'show' in text.lower() or 'display' in text.lower():
                    self.logger.action(f"Clicking submit button by text: {text}")
                    self.driver.execute_script("arguments[0].click();", btn)
                    self.logger.success("Submit clicked!")
                    return True
            
            self.logger.error("Submit button not found!")
            return False
        except Exception as e:
            self.logger.error(f"Submit click failed: {e}")
            return False
    
    def has_table_data(self) -> bool:
        """Check if the display page has actual data in the table"""
        try:
            # Look for common "no data" indicators
            page_text = self.driver.page_source.lower()
            
            no_data_patterns = [
                'no data available',
                'no records found',
                'no records available',  # "There are no records available."
                'there are no records',
                'no data found',
                'data not available',
                'no results',
                'empty result',
                '0 records',
            ]
            
            for pattern in no_data_patterns:
                if pattern in page_text:
                    self.logger.warning(f"No data indicator found: '{pattern}'")
                    return False
        
            # Try to find actual data table with rows
            # Look for table with data rows (not just header)
            tables = self.driver.find_elements(By.CSS_SELECTOR, "table")
            for table in tables:
                try:
                    # Check for data rows (tbody tr or just tr with td)
                    rows = table.find_elements(By.CSS_SELECTOR, "tr")
                    data_rows = 0
                    for row in rows:
                        cells = row.find_elements(By.CSS_SELECTOR, "td")
                        if len(cells) > 1:  # Has multiple cells (likely data row)
                            # Check if cells have actual content
                            has_content = any(cell.text.strip() for cell in cells)
                            if has_content:
                                data_rows += 1
                    
                    if data_rows > 0:
                        self.logger.info(f"Found table with {data_rows} data rows")
                        return True
                except:
                    continue
            
            # Also check if Excel button is present and enabled (good indicator of data)
            try:
                excel_btn = self.driver.find_element(By.ID, ELEMENT_IDS['excel_btn'])
                if excel_btn.is_displayed() and excel_btn.is_enabled():
                    self.logger.info("Excel button is present and enabled - data likely available")
                    return True
            except:
                pass
            
            self.logger.warning("Could not confirm table has data")
            return False
            
        except Exception as e:
            self.logger.warning(f"Error checking table data: {e}")
            return False
    
    def click_excel(self) -> bool:
        """Click excel export"""
        try:
            btn = self.driver.find_element(By.ID, ELEMENT_IDS['excel_btn'])
            btn.click()
            return True
        except:
            return False
    
    def click_back(self) -> bool:
        """Click back button on display page to return to tree page"""
        self.logger.action("Clicking Back button to return to tree page...")
        try:
            # Try multiple back button selectors
            back_selectors = [
                ELEMENT_IDS['btn_back'],
                'ctl00_ContentPlaceHolder1_ibtnback',
                'ctl00_ContentPlaceHolder1_btnBack',
            ]
            
            btn = None
            for sel in back_selectors:
                try:
                    btn = self.driver.find_element(By.ID, sel)
                    if btn.is_displayed():
                        break
                except:
                    continue
            
            if not btn:
                # Try by text/value
                btns = self.driver.find_elements(By.XPATH, "//input[contains(@value, 'Back') or contains(@value, 'back')]")
                if btns:
                    btn = btns[0]
            
            if btn:
                self.driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                time.sleep(0.3)
                self.driver.execute_script("arguments[0].click();", btn)
                self.logger.info("Back button clicked")
                
                # Wait for tree page to load
                time.sleep(2)
                self.wait_for_page()
                
                # Verify we're back on tree page
                for _ in range(10):
                    if self.get_page_type() == 'treeview':
                        self.logger.success("Back on tree page!")
                        return True
                    time.sleep(1)
                
                self.logger.warning("Not on treeview after back click")
                return False
            else:
                self.logger.error("Could not find Back button")
                return False
        except Exception as e:
            self.logger.error(f"click_back error: {e}")
            return False
    
    def are_checkboxes_selected(self, cb_ids: List[str]) -> int:
        """Check how many of the given checkboxes are already selected"""
        count = 0
        for cb_id in cb_ids:
            try:
                elem = self.driver.find_element(By.ID, cb_id)
                if elem.is_selected():
                    count += 1
            except:
                pass
        return count
    
    def wait_for_download(self, timeout: int = 90) -> Optional[Path]:
        """Wait for download to complete"""
        start = time.time()
        initial = set(self.download_dir.glob('*'))
        
        while time.time() - start < timeout:
            current = set(self.download_dir.glob('*'))
            new_files = current - initial
            completed = [f for f in new_files if not f.suffix in ['.crdownload', '.tmp']]
            
            if completed:
                return max(completed, key=lambda f: f.stat().st_mtime)
            time.sleep(1)
        
        return None
    
    def close(self):
        if self.driver:
            self.driver.quit()


# ============================================================================
# EXTRACTOR
# ============================================================================

class ASIResearchExtractor:
    """Main extractor for research data"""
    
    MAX_CHECKBOXES_PER_BATCH = 25  # Website limit for non-state subsections
    MAX_CHECKBOXES_WITH_STATES = 3  # Very small batch when states are involved (site gets very slow with more)
    
    def __init__(self, output_dir: Path, test_mode: bool = False, headless: bool = False,
                 manufacturing_only: bool = True):
        self.output_dir = output_dir
        self.test_mode = test_mode
        self.manufacturing_only = manufacturing_only  # Default: only NIC 10-33
        
        # Create directory structure
        self.raw_dir = output_dir / 'raw'
        self.log_dir = output_dir / 'logs'
        self.db_dir = output_dir / 'database'
        
        for d in [self.raw_dir, self.log_dir, self.db_dir]:
            d.mkdir(parents=True, exist_ok=True)
        
        self.logger = Logger(self.log_dir)
        self.download_dir = output_dir / '_temp_downloads'
        self.download_dir.mkdir(exist_ok=True)
        
        self.browser = Browser(self.download_dir, self.logger, headless)
        
        # Tracking
        self.extraction_log = []
    
    def _extract_nic_codes(self, checkbox_ids: List[str]) -> List[int]:
        """Extract NIC codes from checkbox IDs for file naming"""
        nic_codes = []
        for cb_id in checkbox_ids:
            try:
                # Try to find the associated element and get NIC code from text
                elem = self.browser.driver.find_element(By.ID, cb_id)
                parent_tr = elem.find_element(By.XPATH, "./ancestor::tr[1]")
                spans = parent_tr.find_elements(By.CSS_SELECTOR, "span")
                
                for span in spans:
                    text = span.text.strip()
                    if text and len(text) >= 2 and text[:2].isdigit():
                        nic_code = int(text[:2])
                        if 10 <= nic_code <= 99:  # Valid 2-digit NIC
                            nic_codes.append(nic_code)
                            break
            except Exception:
                # Try parsing from ID (some IDs contain the code)
                import re
                match = re.search(r'_(\d{2})[-_]', cb_id)
                if match:
                    nic_codes.append(int(match.group(1)))
        
        return sorted(set(nic_codes))
    
    def extract_subsection(self, subsection_id: int):
        """Extract all data for a subsection"""
        config = RESEARCH_SUBSECTIONS.get(subsection_id)
        if not config:
            self.logger.error(f"Unknown subsection: {subsection_id}")
            return
        
        self.logger.step("=" * 60)
        self.logger.step(f"EXTRACTING: {config['name']}")
        self.logger.step(f"Description: {config['description']}")
        self.logger.step("=" * 60)
        
        # Create output folder
        subsection_dir = self.raw_dir / config['folder']
        subsection_dir.mkdir(exist_ok=True)
        
        # Navigate
        if not self.browser.navigate_to_asi():
            self.logger.error("Failed to navigate to ASI")
            return
        
        if not self.browser.select_subsection(config['radio_id']):
            self.logger.error("Failed to select subsection")
            return
        
        self.logger.success("On treeview page")
        
        # Get available series - ONLY use EPWRF Concorded Series
        series_list = []
        if config['has_series']:
            series_opts = self.browser.get_dropdown_options(ELEMENT_IDS['series'])
            # Filter to ONLY EPWRF Concorded Series
            epwrf_series = [s['text'] for s in series_opts 
                           if 'EPWRF' in s['text'] or 'Concorded' in s['text'] or 'concorded' in s['text']]
            
            if epwrf_series:
                series_list = epwrf_series
                self.logger.data(f"Using EPWRF Concorded Series: {series_list}")
            else:
                # Fallback: show available and pick latest NIC if no EPWRF
                all_series = [s['text'] for s in series_opts if 'All' not in s['text'] and 'Select' not in s['text']]
                self.logger.warning(f"No EPWRF series found. Available: {all_series}")
                # Try NIC-2008 as fallback (most recent)
                nic_2008 = [s for s in all_series if '2008' in s]
                series_list = nic_2008 if nic_2008 else all_series[-1:]
                self.logger.warning(f"Using fallback series: {series_list}")
        else:
            series_list = [None]
        
        # Test mode limits (only affects batches/variables, not series)
        # We always extract full EPWRF Concorded Series for maximum period
        
        # Extract for each series
        for series in series_list:
            self._extract_series(subsection_id, config, subsection_dir, series)
        
        self.logger.success(f"Completed subsection {subsection_id}")
    
    def _extract_series(self, subsection_id: int, config: Dict, output_dir: Path, series: str):
        """Extract data for one NIC series - ONLY EPWRF Concorded Series"""
        series_name = series.replace(' ', '_').replace('-', '_') if series else 'default'
        series_dir = output_dir / series_name
        series_dir.mkdir(exist_ok=True)
        
        self.logger.step(f"\n--- Series: {series or 'default'} ---")
        
        # Select series
        if series:
            self.browser.select_dropdown(ELEMENT_IDS['series'], series)
            time.sleep(1)
        
        # Set full year range
        year_opts = self.browser.get_dropdown_options(ELEMENT_IDS['start_year'])
        if year_opts:
            self.browser.select_dropdown(ELEMENT_IDS['start_year'], index=0)
            end_opts = self.browser.get_dropdown_options(ELEMENT_IDS['end_year'])
            if end_opts:
                self.browser.select_dropdown(ELEMENT_IDS['end_year'], index=len(end_opts)-1)
                self.logger.data(f"Years: {year_opts[0]['text']} to {end_opts[-1]['text']}")
        
        # IMPORTANT: Expand ONLY the EPWRF Concorded Series node, NOT all nodes
        self.logger.action("Expanding EPWRF Concorded Series node only...")
        if not self.browser.expand_epwrf_node_only():
            self.logger.warning("Could not find EPWRF node - trying full expand as fallback")
            self.browser.expand_tree()
        
        # Get EPWRF checkboxes - manufacturing only or all
        if self.manufacturing_only:
            self.logger.action("Filtering for manufacturing industries (NIC 10-33)...")
            all_checkboxes = self.browser.get_manufacturing_checkboxes()
        else:
            all_checkboxes = self.browser.get_epwrf_checkboxes()
        
        if not all_checkboxes:
            self.logger.warning("No EPWRF checkboxes found - falling back to all checkboxes")
            all_checkboxes = self.browser.get_tree_checkboxes()
        
        self.logger.data(f"Found {len(all_checkboxes)} industry checkboxes to extract")
        
        # Determine batch size based on whether states are involved
        # States + many industries = site gets very slow, so use smaller batches
        if config['has_states']:
            batch_size = self.MAX_CHECKBOXES_WITH_STATES
            self.logger.info(f"Using smaller batch size ({batch_size}) for state-level data")
        else:
            batch_size = self.MAX_CHECKBOXES_PER_BATCH
        
        # Split into batches
        batches = [all_checkboxes[i:i+batch_size] 
                   for i in range(0, len(all_checkboxes), batch_size)]
        
        self.logger.info(f"Split into {len(batches)} batches of max {batch_size} industries each")
        
        # Test mode
        if self.test_mode:
            batches = batches[:1]
            self.logger.warning("TEST MODE: Limited to 1 batch")
        
        # Get variables
        variables = self.browser.get_variables()
        self.logger.data(f"Variables available: {len(variables)}")
        
        # Filter to priority variables
        priority_vars = [v for v in variables if any(p.lower() in v.lower() for p in PRIORITY_VARIABLES)]
        other_vars = [v for v in variables if v not in priority_vars]
        
        self.logger.info(f"Priority variables: {len(priority_vars)}")
        
        # In test mode, only extract priority variables
        if self.test_mode:
            variables = priority_vars[:3] if priority_vars else variables[:3]
            self.logger.warning(f"TEST MODE: Limited to {len(variables)} variables")
        else:
            # Priority first, then others
            variables = priority_vars + other_vars
        
        # Extract each batch
        for batch_idx, batch in enumerate(batches):
            self._extract_batch(config, series_dir, series, batch, batch_idx, len(batches), 
                               variables, subsection_id)
            
            # Return to treeview for next batch
            if batch_idx < len(batches) - 1:
                self.browser.click_back()
                time.sleep(2)
                
                # Re-select series
                if series:
                    self.browser.select_dropdown(ELEMENT_IDS['series'], series)
                    time.sleep(1)
    
    def _extract_batch(self, config: Dict, output_dir: Path, series: str, 
                       batch: List[str], batch_idx: int, total_batches: int, 
                       variables: List[str], subsection_id: int):
        """Extract one batch of checkboxes - iterate through each variable"""
        self.logger.step(f"\n=== Batch {batch_idx+1}/{total_batches} ({len(batch)} industries) ===")
        
        # Extract NIC range from batch checkboxes for file naming
        nic_codes = self._extract_nic_codes(batch)
        if nic_codes:
            nic_range = f"{min(nic_codes)}-{max(nic_codes)}"
        else:
            nic_range = f"batch{batch_idx+1:02d}"
        self.logger.info(f"NIC range for batch: {nic_range}")
        
        # For each variable, we need to:
        # 1. Select checkboxes (on tree page)
        # 2. Select this specific variable
        # 3. Submit to display page
        # 4. Download Excel
        # 5. Go back to tree page for next variable
        
        for var_idx, var_name in enumerate(variables):
            self.logger.step(f"\n--- Variable {var_idx+1}/{len(variables)}: {var_name} ---")
            
            # Make sure we're on tree page
            page_type = self.browser.get_page_type()
            if page_type != 'treeview':
                self.logger.info(f"Currently on: {page_type}, need to go back to treeview")
                # This should not happen after first variable since we click back after download
                # But as fallback, try browser back
                self.browser.driver.back()
                time.sleep(3)
                self.browser.wait_for_page()
            
            # Check if checkboxes are already selected (persisted from previous variable)
            already_selected = self.browser.are_checkboxes_selected(batch)
            self.logger.info(f"Checkboxes already selected: {already_selected}/{len(batch)}")
            
            if already_selected < len(batch):
                # Need to select checkboxes
                self.browser.deselect_all_checkboxes()
                time.sleep(0.3)
                selected = self.browser.select_checkboxes(batch)
                self.logger.data(f"Selected {selected} checkboxes")
            else:
                self.logger.info("All checkboxes still selected from previous variable")
                selected = already_selected
            
            if selected == 0:
                self.logger.warning("No checkboxes selected, skipping")
                continue
            
            # Select states if needed
            if config['has_states']:
                states = self.browser.select_all_states()
                self.logger.data(f"Selected {states} states")
            
            # Select THIS variable
            self.logger.action(f"Selecting variable: {var_name}")
            if not self.browser.select_variable(var_name):
                self.logger.warning(f"Could not select variable: {var_name}, skipping")
                continue
            time.sleep(0.5)
            
            # Submit to display page
            self.logger.action("Submitting to display page...")
            if not self.browser.click_submit():
                self.logger.error("Failed to click submit!")
                continue
            
            time.sleep(3)
            
            # Wait for display page
            self.logger.action("Waiting for display page...")
            if not self.browser.wait_for_display_page(timeout=180):
                self.logger.error("Failed to reach display page")
                # Try to go back
                self.browser.click_back()
                time.sleep(2)
                continue
            
            self.logger.success("Display page loaded!")
            
            # Try to download - if no data, Excel click will just refresh page (no download)
            # This is simpler and more reliable than trying to detect "no data" text
            download_success = self._download_variable(output_dir, series, batch_idx, var_name, var_idx, len(variables),
                                   subsection_id, nic_range, len(batch))
            
            # Go back to tree page for next variable (if not last variable)
            if var_idx < len(variables) - 1:
                self.logger.action("Going back to tree page for next variable...")
                if not self.browser.click_back():
                    self.logger.warning("Back button failed, trying browser back...")
                    self.browser.driver.back()
                    time.sleep(3)
                time.sleep(1)
        
        self.logger.success(f"Batch {batch_idx+1} complete!")
    
    def _download_variable(self, output_dir: Path, series: str, batch_idx: int, 
                           var_name: str, var_idx: int, total_vars: int,
                           subsection_id: int, nic_range: str, num_industries: int) -> bool:
        """Download data for one variable (already on display page)
        
        Returns True if download succeeded, False if no data or failed.
        If no data exists, clicking Excel just refreshes the page (no download).
        """
        self.logger.action(f"Attempting download: {var_name}")
        
        config = RESEARCH_SUBSECTIONS[subsection_id]
        short_code = config['short_code']
        
        # Click Excel to download
        # If no data exists, this will just refresh the page (no file downloaded)
        if not self.browser.click_excel():
            self.logger.error("Excel click failed")
            return False
        
        # Wait for download with shorter timeout (if no data, page refreshes quickly)
        # Use 15 seconds - if no file appears, assume no data
        downloaded = self.browser.wait_for_download(timeout=15)
        
        if not downloaded:
            # No file downloaded - either no data or timeout
            # Check if page refreshed (indicates no data scenario)
            self.logger.warning(f"No data available for '{var_name}' (no download started)")
            return False
        
        # File downloaded - save it
        # Create structured filename: sub{SS}_var{VV}_{variable_name}_batch{BB}_nic{XX-YY}.xls
        clean_name = re.sub(r'[^\w\s-]', '', var_name).replace(' ', '_').lower()
        target_name = f"{short_code}_var{var_idx+1:02d}_{clean_name}_batch{batch_idx+1:02d}_nic{nic_range}.xls"
        target_path = output_dir / target_name
        
        # Move file
        try:
            shutil.move(str(downloaded), str(target_path))
            self.logger.success(f"Saved: {target_name}")
            
            # Enhanced metadata for ETL
            self.extraction_log.append({
                'file_name': target_name,
                'file_path': str(target_path),
                'subsection_id': subsection_id,
                'subsection_code': short_code,
                'subsection_name': config['name'],
                'data_type': config['data_type'],
                'has_states': config['has_states'],
                'series': series,
                'variable_name': var_name,
                'variable_index': var_idx + 1,
                'batch_index': batch_idx + 1,
                'nic_range': nic_range,
                'num_industries': num_industries,
                'timestamp': datetime.now().isoformat()
            })
            return True
        except Exception as e:
            self.logger.error(f"Move failed: {e}")
            return False
    
    def save_extraction_log(self):
        """Save extraction log and manifest to JSON"""
        # Save detailed extraction log (full metadata for ETL)
        log_path = self.output_dir / 'extraction_log.json'
        with open(log_path, 'w') as f:
            json.dump(self.extraction_log, f, indent=2)
        self.logger.info(f"Extraction log saved: {log_path}")
        
        # Create summary manifest
        self._save_manifest()
    
    def _save_manifest(self):
        """Create summary manifest for quick reference"""
        # Group files by subsection
        by_subsection = {}
        for entry in self.extraction_log:
            sub_code = entry.get('subsection_code', 'unknown')
            if sub_code not in by_subsection:
                by_subsection[sub_code] = {
                    'subsection_id': entry.get('subsection_id'),
                    'subsection_name': entry.get('subsection_name'),
                    'data_type': entry.get('data_type'),
                    'has_states': entry.get('has_states'),
                    'series': entry.get('series'),
                    'variables': set(),
                    'nic_ranges': set(),
                    'file_count': 0
                }
            by_subsection[sub_code]['variables'].add(entry.get('variable_name'))
            by_subsection[sub_code]['nic_ranges'].add(entry.get('nic_range'))
            by_subsection[sub_code]['file_count'] += 1
        
        # Convert sets to sorted lists
        for sub in by_subsection.values():
            sub['variables'] = sorted(list(sub['variables']))
            sub['nic_ranges'] = sorted(list(sub['nic_ranges']))
        
        manifest = {
            'extraction_date': datetime.now().isoformat(),
            'total_files': len(self.extraction_log),
            'test_mode': self.test_mode,
            'manufacturing_only': self.manufacturing_only,
            'subsections': by_subsection,
            'file_naming_pattern': 'sub{SS}_var{VV}_{variable_name}_batch{BB}_nic{XX-YY}.xls',
            'directory_structure': {
                'raw': 'Raw downloaded XLS files (HTML table format)',
                'logs': 'Extraction logs with timestamps',
                'database': 'SQLite database (after ETL)',
                'extraction_log.json': 'Full metadata for ETL pipeline',
                'manifest.json': 'This summary file'
            }
        }
        
        manifest_path = self.output_dir / 'manifest.json'
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        self.logger.info(f"Manifest saved: {manifest_path}")
    
    def run(self, subsection_ids: List[int]):
        """Run extraction for specified subsections"""
        self.logger.step("\n" + "=" * 60)
        self.logger.step("ASI RESEARCH DATA EXTRACTOR")
        self.logger.step(f"Subsections: {subsection_ids}")
        self.logger.step(f"Test mode: {self.test_mode}")
        self.logger.step(f"Output: {self.output_dir}")
        self.logger.step("=" * 60 + "\n")
        
        for sid in subsection_ids:
            try:
                self.extract_subsection(sid)
            except Exception as e:
                self.logger.error(f"Subsection {sid} failed: {e}")
        
        self.save_extraction_log()
        self.logger.step("\n" + "=" * 60)
        self.logger.step("EXTRACTION COMPLETE")
        self.logger.step(f"Files extracted: {len(self.extraction_log)}")
        self.logger.step("=" * 60)
    
    def close(self):
        self.browser.close()
        self.logger.close()


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='ASI Research Data Extractor - Manufacturing Industries (NIC 10-33)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Research-relevant subsections:
  2  - 2-Digit Industry Groups (All India) - Main industry data
  7  - 2-Digit Industry by States - Regional analysis
  10 - Employment Characteristics - Detailed employment

Examples:
  # Start with All India 2-digit data
  python asi_research_extractor.py --subsection 2 --output ./asi_research_data
  
  # Extract state-wise data
  python asi_research_extractor.py --subsection 7 --output ./asi_research_data
  
  # Extract all research subsections
  python asi_research_extractor.py --all --output ./asi_research_data
  
  # Test mode (quick validation)
  python asi_research_extractor.py --subsection 2 --test --output ./asi_test
        """
    )
    
    parser.add_argument('--subsection', '-s', type=int, nargs='+',
                        choices=[2, 7, 10, 11], help='Subsection(s) to extract')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Extract all research subsections (2, 7, 10, 11)')
    parser.add_argument('--output', '-o', type=str, default='./asi_research_data',
                        help='Output directory')
    parser.add_argument('--test', '-t', action='store_true',
                        help='Test mode (limited extraction)')
    parser.add_argument('--headless', action='store_true',
                        help='Run browser in headless mode')
    parser.add_argument('--manufacturing', '-m', action='store_true',
                        help='Extract only manufacturing industries (NIC 10-33)')
    parser.add_argument('--all-industries', action='store_true',
                        help='Extract all EPWRF industries (not just manufacturing)')
    
    args = parser.parse_args()
    
    if args.all:
        subsections = [2, 7, 10, 11]
    elif args.subsection:
        subsections = args.subsection
    else:
        print("Error: Specify --subsection or --all")
        parser.print_help()
        return
    
    # Determine industry filter
    manufacturing_only = not args.all_industries  # Default: manufacturing only
    if args.manufacturing:
        manufacturing_only = True
    
    industry_str = "Manufacturing (NIC 10-33)" if manufacturing_only else "All EPWRF Industries"
    
    print(f"""
╔════════════════════════════════════════════════════════════════╗
║         ASI RESEARCH DATA EXTRACTOR                            ║
║         {industry_str:^48} ║
╠════════════════════════════════════════════════════════════════╣
║  Extracts: GVA, Output, Employment, Capital, Wages             ║
║  For: Industrial analysis, productivity, regional patterns     ║
╚════════════════════════════════════════════════════════════════╝
    """)
    
    extractor = ASIResearchExtractor(
        Path(args.output),
        test_mode=args.test,
        headless=args.headless,
        manufacturing_only=manufacturing_only
    )
    
    try:
        extractor.run(subsections)
    except KeyboardInterrupt:
        print("\n\nInterrupted!")
    finally:
        extractor.close()


if __name__ == '__main__':
    main()
