"""
HydroGuard AI — Water Risk Intelligence Platform
Compares ANY uploaded water quality parameters against WHO (2022) guidelines,
flags exceedances, provides mitigation strategies with literature citations,
and generates a detailed PDF report.

Install:
    pip install streamlit pandas numpy matplotlib reportlab openpyxl xlrd odfpy

Run:
    streamlit run hydroguard_app.py

Fixes applied:
    1. Python 3.8 / OpenSSL md5 compatibility patch for ReportLab
    2. PDF bytes cached in session_state to survive Streamlit reruns
    3. Accepts CSV, XLSX, XLS, XLSM, XLSB, ODS file formats
    4. No upload file size limit (set in .streamlit/config.toml)
    5. Expanded ALIASES + smarter normalise_col() with fuzzy matching
    6. Added Sodium (Na) WHO parameter
    7. Added chlorides / sodium aliases
"""

# ══════════════════════════════════════════════════════════════════
# PYTHON 3.8 / OPENSSL COMPATIBILITY PATCH — must be first
# ══════════════════════════════════════════════════════════════════
import hashlib

_orig_md5 = hashlib.md5
def _safe_md5(*args, **kwargs):
    kwargs.pop("usedforsecurity", None)
    return _orig_md5(*args, **kwargs)
hashlib.md5 = _safe_md5

# ══════════════════════════════════════════════════════════════════
# IMPORTS
# ══════════════════════════════════════════════════════════════════
import re
import io
import datetime

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, Image as RLImage, KeepTogether,
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY

# ══════════════════════════════════════════════════════════════════
# WHO GUIDELINES DATABASE
# Source: WHO (2022) Guidelines for Drinking-water Quality, 4th ed.
#         incorporating the 1st and 2nd addenda.
#         Geneva: World Health Organization. ISBN 978-92-4-004506-4
# ══════════════════════════════════════════════════════════════════
WHO_PARAMS = {
    # ── Physical
    "ph": {
        "label": "pH",
        "unit": "",
        "min": 6.5,
        "max": 8.5,
        "who_limit": "6.5 – 8.5 (no health-based guideline; aesthetic range)",
        "who_note": "No health-based guideline value. Values outside 6.5–8.5 may affect taste and corrode pipes.",
        "source": "WHO (2022), Section 12.1; US EPA (2012) Secondary Standards",
        "health_effects": "Low pH causes corrosion of pipes releasing heavy metals (Pb, Cu). High pH causes bitter taste and reduces chlorine efficacy.",
        "category": "Physical",
        "exceedance_type": "range",
        "color": "#185FA5",
        "mitigation": [
            {
                "strategy": "pH correction (acidic water)",
                "action": "Dose lime (Ca(OH)2) or soda ash (Na2CO3) at the treatment plant to raise pH to 7.0–8.0.",
                "reference": "Crittenden et al. (2012) MWH's Water Treatment: Principles and Design, 3rd ed. Wiley.",
            },
            {
                "strategy": "pH correction (alkaline water)",
                "action": "Dose CO2 gas or hydrochloric acid (industrial settings) to lower pH. Ensure controlled addition with in-line monitoring.",
                "reference": "Letterman, R.D. (1999) Water Quality and Treatment: A Handbook of Community Water Supplies, 5th ed. AWWA/McGraw-Hill.",
            },
            {
                "strategy": "Corrosion inhibitor dosing",
                "action": "Apply orthophosphate inhibitors (1–3 mg/L as PO4) to prevent pipe corrosion at low pH.",
                "reference": "WHO (2022) Guidelines for Drinking-water Quality, 4th ed. Chapter 10.",
            },
        ],
    },
    "turbidity": {
        "label": "Turbidity",
        "unit": "NTU",
        "min": None,
        "max": 1.0,
        "who_limit": "< 1 NTU (ideally < 0.1 NTU at point of disinfection)",
        "who_note": "Turbidity > 1 NTU can protect pathogens from disinfection. Values > 4 NTU are unacceptable.",
        "source": "WHO (2022), Section 10.6; WHO (2017) Turbidity Fact Sheet",
        "health_effects": "High turbidity shields pathogens (bacteria, viruses, protozoa) from UV/chlorine disinfection, increasing risk of waterborne disease outbreaks.",
        "category": "Physical",
        "exceedance_type": "max",
        "color": "#BA7517",
        "mitigation": [
            {
                "strategy": "Coagulation and flocculation",
                "action": "Add alum (Al2(SO4)3) or ferric chloride (5–50 mg/L) followed by gentle mixing to aggregate particles. Optimise dose via jar testing.",
                "reference": "Bratby, J. (2016) Coagulation and Flocculation in Water and Wastewater Treatment, 3rd ed. IWA Publishing.",
            },
            {
                "strategy": "Sedimentation / clarification",
                "action": "Follow coagulation with sedimentation (horizontal-flow, inclined plate, or dissolved air flotation) to remove flocs before filtration.",
                "reference": "Crittenden et al. (2012) MWH's Water Treatment: Principles and Design, 3rd ed. Wiley.",
            },
            {
                "strategy": "Rapid sand filtration",
                "action": "Pass coagulated water through rapid sand filters (loading rate 5–15 m/h) to achieve turbidity < 0.1 NTU before disinfection.",
                "reference": "Montgomery Watson Harza (2005) Water Treatment Plant Design, 4th ed. AWWA.",
            },
            {
                "strategy": "Catchment management",
                "action": "Implement riparian buffer zones, check dams, and erosion control to reduce sediment loading at source.",
                "reference": "UNEP (2016) A Snapshot of the World's Water Quality: Advancing a Global Assessment. Nairobi: UNEP.",
            },
        ],
    },
    "temperature": {
        "label": "Temperature",
        "unit": "°C",
        "min": None,
        "max": 25.0,
        "who_limit": "< 25°C (no formal guideline; aesthetic/operational threshold)",
        "who_note": "Elevated temperatures accelerate microbial growth, reduce dissolved oxygen, and increase taste/odour problems.",
        "source": "WHO (2022), Section 12.1; Edberg et al. (2000) IJEH",
        "health_effects": "Temperatures above 25°C promote growth of Legionella spp. and other opportunistic pathogens. Reduces effectiveness of chlorine disinfection.",
        "category": "Physical",
        "exceedance_type": "max",
        "color": "#E24B4A",
        "mitigation": [
            {
                "strategy": "Storage and distribution insulation",
                "action": "Insulate storage tanks and bury distribution pipes to minimise solar heating. Maintain water age < 24 h in distribution.",
                "reference": "Rossman, L.A. (2000) EPANET 2 Users Manual. US EPA Office of Research and Development.",
            },
            {
                "strategy": "Pre-dawn abstraction",
                "action": "For surface sources, abstract water in early morning hours when temperatures are lowest to reduce raw water temperature load.",
                "reference": "Caissie, D. (2006) The thermal regime of rivers: A review. Freshwater Biology, 51(8), 1389–1406.",
            },
            {
                "strategy": "Riparian shading",
                "action": "Plant or restore riparian vegetation along watercourses to reduce solar radiation reaching the water surface.",
                "reference": "Johnson, S.L. (2004) Factors influencing stream temperatures in small streams. Hydrological Processes, 18(1), 75–88.",
            },
        ],
    },
    "dissolved_oxygen": {
        "label": "Dissolved Oxygen",
        "unit": "mg/L",
        "min": 6.0,
        "max": None,
        "who_limit": "> 6.0 mg/L (ecological guideline for aquatic life)",
        "who_note": "WHO does not set a drinking water limit for DO, but values < 6 mg/L indicate organic pollution and anaerobic conditions threatening aquatic ecosystems.",
        "source": "USEPA (2000) Ambient Water Quality Criteria for Dissolved Oxygen; EU WFD (2000/60/EC)",
        "health_effects": "Low DO promotes anaerobic decomposition, producing H2S, methane, and mobilising heavy metals (Fe, Mn, As) from sediments.",
        "category": "Chemical",
        "exceedance_type": "min",
        "color": "#0F6E56",
        "mitigation": [
            {
                "strategy": "Aeration",
                "action": "Install cascade aerators, mechanical surface aerators, or diffused air systems to restore DO > 6 mg/L in reservoirs and treatment works.",
                "reference": "Tchobanoglous, G. et al. (2014) Wastewater Engineering: Treatment and Resource Recovery, 5th ed. McGraw-Hill.",
            },
            {
                "strategy": "Reduce organic loading",
                "action": "Control agricultural runoff (fertilisers, manure) and treat wastewater effluents to reduce BOD entering the water body.",
                "reference": "Smith, V.H. et al. (2006) Eutrophication of freshwater and coastal marine ecosystems. Environmental Science & Pollution Research, 13(4), 126–139.",
            },
            {
                "strategy": "Constructed wetlands",
                "action": "Use subsurface-flow constructed wetlands as a buffer to oxidise organic matter before water enters the main body.",
                "reference": "Kadlec, R.H. & Wallace, S.D. (2009) Treatment Wetlands, 2nd ed. CRC Press.",
            },
        ],
    },
    "conductivity": {
        "label": "Electrical Conductivity",
        "unit": "µS/cm",
        "min": None,
        "max": 2500.0,
        "who_limit": "< 2500 µS/cm (aesthetic guideline)",
        "who_note": "High conductivity indicates elevated dissolved solids. Values > 2500 µS/cm cause unacceptable taste and may indicate ion contamination.",
        "source": "WHO (2022), Section 12.1; WHO TDS guideline 1000 mg/L",
        "health_effects": "Correlates with total dissolved solids (TDS). Elevated TDS from Na, Cl, SO4 can affect kidney function with chronic exposure.",
        "category": "Physical",
        "exceedance_type": "max",
        "color": "#7F77DD",
        "mitigation": [
            {
                "strategy": "Reverse osmosis (RO)",
                "action": "Deploy RO membranes (rejection rate 95–99%) to reduce TDS and conductivity. Suitable for high-salinity or brackish sources.",
                "reference": "Metcalf & Eddy (2007) Water Reuse: Issues, Technologies, and Applications. McGraw-Hill.",
            },
            {
                "strategy": "Electrodialysis reversal (EDR)",
                "action": "Use EDR for moderate salinity waters (500–3000 mg/L TDS) where RO energy costs are prohibitive.",
                "reference": "Xu, T. & Huang, C. (2008) Electrodialysis-based separation technologies: A critical review. AIChE Journal, 54(12), 3147–3159.",
            },
        ],
    },
    "tds": {
        "label": "Total Dissolved Solids",
        "unit": "mg/L",
        "min": None,
        "max": 1000.0,
        "who_limit": "< 1000 mg/L (palatability guideline)",
        "who_note": "TDS > 1000 mg/L is unpalatable. > 1200 mg/L may cause laxative effects. Ideal < 600 mg/L.",
        "source": "WHO (2022), Section 12.1",
        "health_effects": "Excess TDS from sulfates and magnesium causes diarrhoea. Elevated sodium contributes to hypertension.",
        "category": "Physical",
        "exceedance_type": "max",
        "color": "#888780",
        "mitigation": [
            {
                "strategy": "Reverse osmosis",
                "action": "RO systems can reduce TDS by 90–99%. Requires pre-treatment (sediment + carbon filters) to protect membranes.",
                "reference": "Crittenden et al. (2012) MWH's Water Treatment, 3rd ed. Wiley.",
            },
            {
                "strategy": "Blending",
                "action": "Blend high-TDS source water with a low-TDS source to achieve compliant mixed supply. Requires real-time conductivity monitoring.",
                "reference": "Letterman, R.D. (1999) Water Quality and Treatment, 5th ed. AWWA/McGraw-Hill.",
            },
        ],
    },
    "sodium": {
        "label": "Sodium (Na)",
        "unit": "mg/L",
        "min": None,
        "max": 200.0,
        "who_limit": "< 200 mg/L (aesthetic/taste guideline)",
        "who_note": "No formal health-based guideline. Values > 200 mg/L cause salty taste. Relevant for hypertensive individuals on sodium-restricted diets.",
        "source": "WHO (2022), Section 12.1; US EPA Secondary Standard",
        "health_effects": "Chronic high sodium intake linked to hypertension and cardiovascular disease. Particularly concerning for infants and patients with kidney disease or heart failure.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#F59E0B",
        "mitigation": [
            {
                "strategy": "Reverse osmosis",
                "action": "RO membranes reject 90–98% of sodium ions. Preferred for high-sodium groundwater or brackish sources.",
                "reference": "Crittenden et al. (2012) MWH's Water Treatment: Principles and Design, 3rd ed. Wiley.",
            },
            {
                "strategy": "Electrodialysis reversal (EDR)",
                "action": "EDR selectively removes ionic species including Na+. Cost-effective for moderate salinity (200–1000 mg/L TDS).",
                "reference": "Xu, T. & Huang, C. (2008) Electrodialysis-based separation technologies. AIChE Journal, 54(12), 3147–3159.",
            },
            {
                "strategy": "Blend with low-sodium source",
                "action": "Mix high-sodium source water with a compliant low-sodium source to dilute below 200 mg/L. Monitor continuously.",
                "reference": "WHO (2022) Guidelines for Drinking-water Quality, 4th ed. Geneva: WHO.",
            },
        ],
    },
    "nitrate": {
        "label": "Nitrate (NO3-)",
        "unit": "mg/L",
        "min": None,
        "max": 50.0,
        "who_limit": "< 50 mg/L NO3- (or 11 mg/L as NO3-N)",
        "who_note": "Critical for infants < 3 months — causes methaemoglobinaemia (blue baby syndrome). Also associated with colorectal cancer.",
        "source": "WHO (2022), Section 7.4; EU Drinking Water Directive (98/83/EC)",
        "health_effects": "Reduces oxygen-carrying capacity of blood (methaemoglobinaemia) in infants. Chronic exposure linked to thyroid disruption and cancer.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#639922",
        "mitigation": [
            {
                "strategy": "Ion exchange",
                "action": "Use anion exchange resins (strong base type II) to selectively remove nitrate. Regenerate with NaCl brine.",
                "reference": "Kapoor, A. & Viraraghavan, T. (1997) Nitrate Removal from Drinking Water. Journal of Environmental Engineering, 123(4), 371–380.",
            },
            {
                "strategy": "Biological denitrification",
                "action": "Install biofilters with methanol or hydrogen as electron donor to convert NO3- to N2 gas. Effective for large-scale treatment.",
                "reference": "Knowles, R. (1982) Denitrification. Microbiological Reviews, 46(1), 43–70.",
            },
            {
                "strategy": "Agricultural best management practices",
                "action": "Reduce fertiliser application rates, use slow-release fertilisers, install riparian buffer strips, and implement precision agriculture.",
                "reference": "UNEP (2016) A Snapshot of the World's Water Quality. Nairobi: UNEP.",
            },
            {
                "strategy": "Reverse osmosis",
                "action": "RO achieves 85–95% nitrate removal. Preferred for small or domestic-scale systems where ion exchange is not feasible.",
                "reference": "Crittenden et al. (2012) MWH's Water Treatment, 3rd ed. Wiley.",
            },
        ],
    },
    "nitrite": {
        "label": "Nitrite (NO2-)",
        "unit": "mg/L",
        "min": None,
        "max": 3.0,
        "who_limit": "< 3 mg/L (short-term); < 0.2 mg/L (long-term)",
        "who_note": "More toxic than nitrate on a molar basis. Indicates incomplete nitrification in treatment or distribution.",
        "source": "WHO (2022), Section 7.4",
        "health_effects": "Causes methaemoglobinaemia. Reacts with amines to form carcinogenic nitrosamines.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#97C459",
        "mitigation": [
            {
                "strategy": "Optimise nitrification in treatment",
                "action": "Ensure complete biological nitrification in wastewater treatment. Monitor nitrifier activity and maintain adequate DO (> 2 mg/L) in nitrification zones.",
                "reference": "Tchobanoglous, G. et al. (2014) Wastewater Engineering, 5th ed. McGraw-Hill.",
            },
            {
                "strategy": "Chloramination control",
                "action": "Maintain chloramine residual in distribution to prevent nitrite formation from nitrification. Target Cl2:NH3-N ratio > 5:1.",
                "reference": "AWWA (2006) Fundamentals and Control of Nitrification in Chloraminated Drinking Water Distribution Systems.",
            },
        ],
    },
    "ammonia": {
        "label": "Ammonia (NH3)",
        "unit": "mg/L",
        "min": None,
        "max": 1.5,
        "who_limit": "< 1.5 mg/L (aesthetic/odour guideline)",
        "who_note": "Not a direct health concern at guideline levels, but indicates faecal or industrial pollution and interferes with disinfection.",
        "source": "WHO (2022), Section 12.1",
        "health_effects": "Ammonia reacts with chlorine to form chloramines, reducing disinfection efficacy. High levels indicate sewage contamination.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#D85A30",
        "mitigation": [
            {
                "strategy": "Breakpoint chlorination",
                "action": "Add sufficient chlorine to pass the breakpoint (Cl2:NH3 mass ratio ~ 7.6:1) to oxidise all ammonia and establish free chlorine residual.",
                "reference": "Letterman, R.D. (1999) Water Quality and Treatment, 5th ed. AWWA/McGraw-Hill.",
            },
            {
                "strategy": "Biological nitrification filter",
                "action": "Pass water through a biologically active slow sand filter or biofilter to convert NH3 to NO3- prior to disinfection.",
                "reference": "Mouchet, P. (1992) From Conventional to Biological Removal of Iron and Manganese. AWWA Journal, 84(4), 158–167.",
            },
            {
                "strategy": "Source control",
                "action": "Eliminate or treat upstream discharges (septic tanks, wastewater effluents, animal husbandry runoff) contributing ammonia to the source water.",
                "reference": "UNEP (2016) A Snapshot of the World's Water Quality. Nairobi: UNEP.",
            },
        ],
    },
    "fluoride": {
        "label": "Fluoride (F-)",
        "unit": "mg/L",
        "min": None,
        "max": 1.5,
        "who_limit": "< 1.5 mg/L",
        "who_note": "Dental fluorosis occurs > 1.5 mg/L. Skeletal fluorosis at > 4 mg/L with prolonged exposure. Natural levels vary widely.",
        "source": "WHO (2022), Section 9.2",
        "health_effects": "Dental and skeletal fluorosis. Excess fluoride disrupts bone mineralisation and may affect thyroid function with chronic exposure.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#D4537E",
        "mitigation": [
            {
                "strategy": "Activated alumina adsorption",
                "action": "Pass water through activated alumina (Al2O3) beds at pH 5.5–6.0. Regenerate with NaOH (1–4%) solution.",
                "reference": "Jadhav, S.V. et al. (2015) Fluoride in drinking water: Health effects and removal techniques. Reviews on Environmental Health, 30(4), 233–252.",
            },
            {
                "strategy": "Reverse osmosis",
                "action": "RO removes 90–95% of fluoride. Cost-effective at household and community scale in endemic areas.",
                "reference": "Crittenden et al. (2012) MWH's Water Treatment, 3rd ed. Wiley.",
            },
            {
                "strategy": "Coagulation with alum",
                "action": "Alum coagulation at pH 6–7 can remove 60–80% of fluoride through co-precipitation with Al(OH)3 flocs.",
                "reference": "Dahi, E. et al. (1996) Defluoridation of water using bone char. Proceedings of the 22nd WEDC Conference, New Delhi.",
            },
            {
                "strategy": "Blend with low-fluoride source",
                "action": "Blend high-fluoride groundwater with a compliant surface water or rainwater source to dilute below the guideline value.",
                "reference": "WHO (2022) Guidelines for Drinking-water Quality, 4th ed.",
            },
        ],
    },
    "iron": {
        "label": "Iron (Fe)",
        "unit": "mg/L",
        "min": None,
        "max": 0.3,
        "who_limit": "< 0.3 mg/L (aesthetic guideline)",
        "who_note": "No formal health-based guideline. > 0.3 mg/L causes staining, metallic taste, and supports bacterial growth in pipes.",
        "source": "WHO (2022), Section 12.1",
        "health_effects": "Staining of laundry and fixtures. Promotes growth of iron bacteria (Gallionella, Leptothrix) in distribution. Iron overload is a concern only at very high levels.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#BA7517",
        "mitigation": [
            {
                "strategy": "Aeration and filtration",
                "action": "Aerate water to oxidise Fe2+ to Fe3+, then filter through rapid sand or pressure filters. Effective for Fe > 1 mg/L.",
                "reference": "Mouchet, P. (1992) From Conventional to Biological Removal of Iron and Manganese. AWWA Journal, 84(4), 158–167.",
            },
            {
                "strategy": "Biological iron removal",
                "action": "Use biologically active filters (BAF) colonised with Gallionella or Leptothrix bacteria for low-energy iron removal at pH 6.5–8.0.",
                "reference": "Tekerlekopoulou, A.G. et al. (2013) Removal of ammonium, iron and manganese from potable water in biofiltration units. Journal of Chemical Technology & Biotechnology, 88(8), 1387–1408.",
            },
            {
                "strategy": "Greensand filtration",
                "action": "Use manganese greensand (glauconite coated with MnO2) to oxidise and filter iron simultaneously. Regenerate with KMnO4.",
                "reference": "Crittenden et al. (2012) MWH's Water Treatment, 3rd ed. Wiley.",
            },
        ],
    },
    "manganese": {
        "label": "Manganese (Mn)",
        "unit": "mg/L",
        "min": None,
        "max": 0.4,
        "who_limit": "< 0.4 mg/L (health-based); < 0.1 mg/L (aesthetic)",
        "who_note": "Health-based guideline of 0.4 mg/L introduced in 2011. Neurological effects in children with chronic exposure.",
        "source": "WHO (2022), Section 12.1; Wasserman et al. (2006) Environ Health Perspect",
        "health_effects": "Chronic exposure linked to neurotoxicity, especially in children (IQ reduction). Manganese deposits in distribution pipes causing black water incidents.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#533AB7",
        "mitigation": [
            {
                "strategy": "Oxidation and filtration",
                "action": "Oxidise Mn2+ using chlorine, KMnO4, or ozone (most effective at pH > 7.5), then remove by sand or greensand filtration.",
                "reference": "Crittenden et al. (2012) MWH's Water Treatment, 3rd ed. Wiley.",
            },
            {
                "strategy": "Biological manganese removal",
                "action": "Establish biofilters with Mn-oxidising bacteria at pH 7–8. Lower operating cost than chemical oxidation for moderate concentrations.",
                "reference": "Tekerlekopoulou, A.G. et al. (2013) Journal of Chemical Technology & Biotechnology, 88(8), 1387–1408.",
            },
        ],
    },
    "arsenic": {
        "label": "Arsenic (As)",
        "unit": "µg/L",
        "min": None,
        "max": 10.0,
        "who_limit": "< 10 µg/L",
        "who_note": "Provisional guideline — achievability limit. IARC Group 1 carcinogen. Naturally occurring in volcanic and sedimentary rocks.",
        "source": "WHO (2022), Section 8.1; IARC Monograph 100C",
        "health_effects": "Skin lesions (keratosis), bladder, lung, and skin cancer with chronic exposure. Peripheral neuropathy and cardiovascular disease.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#A32D2D",
        "mitigation": [
            {
                "strategy": "Oxidation + coagulation-filtration",
                "action": "Oxidise As(III) to As(V) with chlorine or ozone, then co-precipitate with Fe3+ coagulant. Achieves > 90% removal.",
                "reference": "WHO (2011) Arsenic in Drinking Water, Fact Sheet No. 210. Geneva: WHO.",
            },
            {
                "strategy": "Adsorption on iron-based media",
                "action": "Use iron-coated sand, granular ferric hydroxide (GFH), or iron oxide-coated activated alumina for point-of-use or community systems.",
                "reference": "Mohan, D. & Pittman, C.U. (2007) Arsenic removal from water using adsorbents. Journal of Hazardous Materials, 142(1-2), 1–53.",
            },
            {
                "strategy": "Reverse osmosis",
                "action": "RO achieves 95–99% As removal and is suited to small community or household scale.",
                "reference": "Crittenden et al. (2012) MWH's Water Treatment, 3rd ed. Wiley.",
            },
            {
                "strategy": "Alternative source development",
                "action": "In endemic areas, shift abstraction to shallow dug wells or rainwater harvesting to avoid deep anaerobic aquifer zones with high As.",
                "reference": "van Geen, A. et al. (2002) Promotion of well-switching to mitigate the current arsenic crisis. Bulletin of the World Health Organization, 80(9), 732–737.",
            },
        ],
    },
    "lead": {
        "label": "Lead (Pb)",
        "unit": "µg/L",
        "min": None,
        "max": 10.0,
        "who_limit": "< 10 µg/L",
        "who_note": "No safe level. Guideline is a performance target based on ALARA (as low as reasonably achievable). Primary concern is household plumbing.",
        "source": "WHO (2022), Section 8.9; IARC Group 2A",
        "health_effects": "Neurotoxic — IQ loss and behavioural effects in children at any level of exposure. Kidney damage and hypertension in adults.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#501313",
        "mitigation": [
            {
                "strategy": "Corrosion control",
                "action": "Maintain pH 7.5–8.0 and apply orthophosphate inhibitors (1–3 mg/L) to form insoluble lead phosphate coating on pipe surfaces.",
                "reference": "Hulsmann, A.D. (1990) Particulate lead in water supplies. Water and Environment Journal, 4(1), 19–25.",
            },
            {
                "strategy": "Lead service line replacement",
                "action": "Replace lead service lines, solder joints, and lead-containing brass fittings — the most effective long-term solution.",
                "reference": "US EPA (2021) Lead and Copper Rule Revisions. Federal Register.",
            },
            {
                "strategy": "Point-of-use treatment",
                "action": "Install NSF/ANSI 53-certified pitcher filters (activated carbon block) or RO systems at the tap for high-risk households.",
                "reference": "NSF International (2019) NSF/ANSI Standard 53: Drinking Water Treatment Units — Health Effects.",
            },
        ],
    },
    "e_coli": {
        "label": "E. coli / Faecal coliforms",
        "unit": "CFU/100mL",
        "min": None,
        "max": 0.0,
        "who_limit": "0 CFU/100 mL (must not be detectable)",
        "who_note": "Any detection is unacceptable. E. coli is the primary indicator of faecal contamination of drinking water.",
        "source": "WHO (2022), Section 4.1; Bartram & Rees (2000) Monitoring Bathing Waters",
        "health_effects": "Indicator of faecal contamination; associated pathogens include Salmonella, Vibrio cholerae, Cryptosporidium, and hepatitis A virus causing diarrhoeal disease.",
        "category": "Microbiological",
        "exceedance_type": "max",
        "color": "#E24B4A",
        "mitigation": [
            {
                "strategy": "Chlorination",
                "action": "Maintain free chlorine residual 0.2–0.5 mg/L at point of use. Use on-site sodium hypochlorite generation for remote settings.",
                "reference": "WHO (2022) Guidelines for Drinking-water Quality, 4th ed., Chapter 7.",
            },
            {
                "strategy": "UV disinfection",
                "action": "Apply UV-C at dose >= 40 mJ/cm2 for 4-log inactivation of E. coli. Low chemical input; effective for clear water (turbidity < 1 NTU).",
                "reference": "Hijnen, W.A.M. et al. (2006) Inactivation credit of UV radiation. Water Research, 40(1), 3–22.",
            },
            {
                "strategy": "Boiling",
                "action": "Bring water to a rolling boil for 1 minute (3 minutes at altitude > 2000 m) as an emergency or household-scale intervention.",
                "reference": "WHO (2015) Boiling of Water for Household Drinking Water Safety. WHO Technical Note.",
            },
            {
                "strategy": "Household water treatment and safe storage (HWTS)",
                "action": "Deploy ceramic pot filters, biosand filters, or SODIS (solar disinfection) for community settings without centralised treatment.",
                "reference": "Peter-Varbanets, M. et al. (2009) Decentralised systems for potable water. Water Research, 43(2), 245–265.",
            },
            {
                "strategy": "Catchment sanitation improvement",
                "action": "Eliminate open defecation, improve latrine coverage, and enforce setback distances (> 30 m) between sanitation facilities and water sources.",
                "reference": "Howard, G. & Bartram, J. (2003) Domestic Water Quantity, Service Level and Health. WHO/SDE/WSH/03.02.",
            },
        ],
    },
    "total_coliforms": {
        "label": "Total Coliforms",
        "unit": "CFU/100mL",
        "min": None,
        "max": 0.0,
        "who_limit": "0 CFU/100 mL in treated water; occasional in source water monitoring",
        "who_note": "Must not be detectable in 95% of samples per month for treated supplies. Indicator of general treatment efficacy.",
        "source": "WHO (2022), Section 4.1",
        "health_effects": "Indicates inadequate treatment or post-treatment contamination. Associated with gastroenteritis outbreaks.",
        "category": "Microbiological",
        "exceedance_type": "max",
        "color": "#D85A30",
        "mitigation": [
            {
                "strategy": "Enhanced disinfection",
                "action": "Review chlorine contact time (CT) and residual. Increase dosing or contact time to achieve required log inactivation.",
                "reference": "WHO (2022) Guidelines for Drinking-water Quality, 4th ed., Chapter 7.",
            },
            {
                "strategy": "Distribution system integrity check",
                "action": "Inspect for cross-connections, pressure transients, and intrusion points in the distribution network.",
                "reference": "Mays, L.W. (2004) Water Distribution Systems Handbook. McGraw-Hill.",
            },
        ],
    },
    "hardness": {
        "label": "Total Hardness (as CaCO3)",
        "unit": "mg/L",
        "min": None,
        "max": 500.0,
        "who_limit": "< 500 mg/L CaCO3 (no health-based guideline; aesthetic)",
        "who_note": "Hard water (> 200 mg/L) causes scale in pipes and appliances. Some evidence of cardiovascular benefit at moderate hardness.",
        "source": "WHO (2022), Section 12.1; Sengupta, P. (2013) J. Environ. Public Health",
        "health_effects": "Scale formation reduces pipe diameter and energy efficiency. Very soft water (< 50 mg/L) is corrosive to metals and may lack beneficial Ca and Mg.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#5DCAA5",
        "mitigation": [
            {
                "strategy": "Lime-soda softening",
                "action": "Add lime (Ca(OH)2) and soda ash (Na2CO3) to precipitate CaCO3 and Mg(OH)2. Suitable for large-scale centralised treatment.",
                "reference": "Crittenden et al. (2012) MWH's Water Treatment, 3rd ed. Wiley.",
            },
            {
                "strategy": "Ion exchange softening",
                "action": "Pass water through strong acid cation exchange resin (Na+ form) to replace Ca2+ and Mg2+. Regenerate with NaCl brine.",
                "reference": "Letterman, R.D. (1999) Water Quality and Treatment, 5th ed. AWWA.",
            },
        ],
    },
    "sulfate": {
        "label": "Sulfate (SO4²-)",
        "unit": "mg/L",
        "min": None,
        "max": 500.0,
        "who_limit": "< 500 mg/L (taste/laxative threshold); < 250 mg/L preferred",
        "who_note": "Causes cathartic effects above 500 mg/L. Contributes to corrosion of concrete pipes. No carcinogenic concern.",
        "source": "WHO (2022), Section 12.1; US EPA Secondary Standard 250 mg/L",
        "health_effects": "Diarrhoea and dehydration at high concentrations. Sulfate-reducing bacteria produce H2S, causing corrosion and odour.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#F0997B",
        "mitigation": [
            {
                "strategy": "Reverse osmosis / nanofiltration",
                "action": "NF membranes (MWCO 200–1000 Da) achieve > 95% SO4 rejection with lower energy than RO. Preferred for sulfate-dominated waters.",
                "reference": "Van der Bruggen, B. et al. (2003) Nanofiltration as a treatment method. Separation and Purification Technology, 33(1), 69–80.",
            },
            {
                "strategy": "Blending",
                "action": "Blend with low-sulfate source water to achieve < 250 mg/L. Requires consistent monitoring of both source qualities.",
                "reference": "WHO (2022) Guidelines for Drinking-water Quality, 4th ed.",
            },
        ],
    },
    "chloride": {
        "label": "Chloride (Cl-)",
        "unit": "mg/L",
        "min": None,
        "max": 250.0,
        "who_limit": "< 250 mg/L (taste threshold)",
        "who_note": "No health-based guideline. Salty taste above 250 mg/L. High chloride indicates seawater intrusion, road salt or industrial contamination.",
        "source": "WHO (2022), Section 12.1; US EPA Secondary Standard",
        "health_effects": "Chloride itself is relatively non-toxic but indicates saline intrusion. Accelerates corrosion of metallic distribution infrastructure.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#AFA9EC",
        "mitigation": [
            {
                "strategy": "Desalination (RO/ED)",
                "action": "For seawater or highly saline groundwater intrusion, deploy RO or electrodialysis to reduce Cl- to < 250 mg/L.",
                "reference": "Shannon, M.A. et al. (2008) Science and technology for water purification. Nature, 452, 301–310.",
            },
            {
                "strategy": "Source protection",
                "action": "Protect coastal aquifers from over-abstraction causing seawater intrusion. Implement managed aquifer recharge (MAR).",
                "reference": "Custodio, E. (2002) Aquifer overexploitation: What does it mean? Hydrogeology Journal, 10(2), 254–277.",
            },
        ],
    },
    "bod": {
        "label": "Biochemical Oxygen Demand",
        "unit": "mg/L",
        "min": None,
        "max": 5.0,
        "who_limit": "< 5 mg/L (EU WFD Class 2 standard for surface waters)",
        "who_note": "Not a WHO drinking water parameter; used for surface/effluent water quality assessment.",
        "source": "EU Water Framework Directive 2000/60/EC; APHA Standard Methods",
        "health_effects": "High BOD indicates organic pollution, leading to oxygen depletion, fish kills, and proliferation of pathogenic microorganisms.",
        "category": "Chemical",
        "exceedance_type": "max",
        "color": "#D4537E",
        "mitigation": [
            {
                "strategy": "Activated sludge treatment",
                "action": "Apply aerobic biological treatment (activated sludge or extended aeration) to reduce BOD by 85–95% before discharge.",
                "reference": "Tchobanoglous, G. et al. (2014) Wastewater Engineering, 5th ed. McGraw-Hill.",
            },
            {
                "strategy": "Wetland treatment",
                "action": "Use constructed or natural wetlands as a low-cost polishing step to reduce remaining BOD in treated effluents.",
                "reference": "Kadlec, R.H. & Wallace, S.D. (2009) Treatment Wetlands, 2nd ed. CRC Press.",
            },
        ],
    },
}

# ══════════════════════════════════════════════════════════════════
# ALIASES — expanded with sodium, chlorides, and common variants
# ══════════════════════════════════════════════════════════════════
ALIASES = {
    # pH
    "ph": "ph", "ph_level": "ph", "acidity": "ph", "hydrogen_ion": "ph",
    # Temperature
    "temp": "temperature", "temp_c": "temperature", "water_temp": "temperature",
    "temperature_c": "temperature", "temp_celsius": "temperature",
    # Turbidity
    "turb": "turbidity", "ntu": "turbidity", "turbidity_ntu": "turbidity",
    # Dissolved oxygen
    "do": "dissolved_oxygen", "do_mg_l": "dissolved_oxygen",
    "oxygen": "dissolved_oxygen", "dissolved_o2": "dissolved_oxygen",
    "do_mgl": "dissolved_oxygen", "d_o": "dissolved_oxygen",
    # Conductivity
    "ec": "conductivity", "electrical_conductivity": "conductivity",
    "conductivity_us_cm": "conductivity", "cond": "conductivity",
    "ec_us_cm": "conductivity", "specific_conductance": "conductivity",
    "tec": "conductivity",
    # TDS
    "total_dissolved_solids": "tds", "tds_mg_l": "tds", "tds_mgl": "tds",
    # Sodium
    "sodium": "sodium", "na": "sodium", "na_mg_l": "sodium",
    "sodium_mg_l": "sodium", "na_mgl": "sodium", "sodium_na": "sodium",
    # Nitrate
    "no3": "nitrate", "nitrate_n": "nitrate", "no3_n": "nitrate",
    "no3_mg_l": "nitrate", "nitrate_mg_l": "nitrate", "no3_": "nitrate",
    # Nitrite
    "no2": "nitrite", "no2_mg_l": "nitrite", "nitrite_mg_l": "nitrite", "no2_": "nitrite",
    # Ammonia
    "nh3": "ammonia", "nh4": "ammonia", "ammonia_n": "ammonia",
    "nh4_n": "ammonia", "ammonium": "ammonia", "nh3_mg_l": "ammonia",
    # Fluoride
    "f": "fluoride", "fluoride_mg_l": "fluoride", "f_": "fluoride",
    # Iron
    "fe": "iron", "iron_total": "iron", "fe_mg_l": "iron",
    "total_iron": "iron", "iron_mg_l": "iron",
    # Manganese
    "mn": "manganese", "mn_mg_l": "manganese", "manganese_mg_l": "manganese",
    # Arsenic
    "as": "arsenic", "as_ug_l": "arsenic", "arsenic_ug_l": "arsenic",
    "arsenic_ppb": "arsenic",
    # Lead
    "pb": "lead", "pb_ug_l": "lead", "lead_ug_l": "lead", "lead_ppb": "lead",
    # E. coli
    "ecoli": "e_coli", "e_coli_cfu": "e_coli", "fecal_coliform": "e_coli",
    "faecal_coliform": "e_coli", "escherichia_coli": "e_coli",
    "e_coli_cfu_100ml": "e_coli", "ecoli_cfu_100ml": "e_coli",
    "fecal_coliforms": "e_coli", "faecal_coliforms": "e_coli",
    "e_coli": "e_coli",
    # Total coliforms
    "coliforms": "total_coliforms", "total_coliform": "total_coliforms",
    "tc": "total_coliforms", "coliform": "total_coliforms",
    "total_coliforms_cfu": "total_coliforms",
    # Hardness
    "hardness_caco3": "hardness", "total_hardness": "hardness",
    "hardness_mg_l": "hardness", "th": "hardness",
    # Sulfate
    "so4": "sulfate", "so4_mg_l": "sulfate", "sulphate": "sulfate",
    "so42_": "sulfate", "sulfate_mg_l": "sulfate", "sulphate_mg_l": "sulfate",
    # Chloride — including "chlorides" plural
    "cl": "chloride", "cl_mg_l": "chloride", "chloride_mg_l": "chloride",
    "cl_": "chloride", "chlorides": "chloride", "chlorides_mg_l": "chloride",
    # BOD
    "bod5": "bod", "bod_mg_l": "bod", "biochemical_oxygen_demand": "bod",
}

BG_MAP = {
    "Compliant": "#EAF3DE", "Watch": "#FFF9E6",
    "Exceeded":  "#FCEBEB", "Critical": "#F7C1C1",
}
FG_MAP = {
    "Compliant": "#3B6D11", "Watch": "#7D5A00",
    "Exceeded":  "#A32D2D", "Critical": "#701010",
}

# ══════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ══════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="HydroGuard AI", page_icon="💧", layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    #MainMenu, footer {visibility:hidden;}
    .hg-title {font-size:2.2rem;font-weight:800;color:#0F6E56;margin-bottom:0;}
    .hg-sub   {font-size:1rem;color:#6b7280;margin-top:0;}
    .step-pill{display:inline-block;background:#E1F5EE;color:#0F6E56;border-radius:20px;
               padding:3px 14px;font-size:0.82rem;font-weight:600;margin-bottom:8px;}
    .param-card{border:1px solid #e5e7eb;border-radius:10px;padding:14px;margin-bottom:10px;}
    .who-box{background:#F0F9FF;border-left:4px solid #185FA5;padding:10px 14px;
             border-radius:0 8px 8px 0;margin:8px 0;}
    .ref-box{background:#F9FAFB;border-left:3px solid #9ca3af;padding:8px 12px;
             font-size:0.82rem;color:#6b7280;border-radius:0 6px 6px 0;margin-top:6px;}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════════════
for k, v in {"step": "upload", "df_raw": None, "analysis": None,
             "pdf_bytes": None, "pdf_analysis_id": None}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ══════════════════════════════════════════════════════════════════
# SAMPLE DATA
# ══════════════════════════════════════════════════════════════════
SAMPLE = pd.DataFrame({
    "ph":               [7.2, 5.8, 8.9, 6.3, 9.3, 4.9, 7.1],
    "turbidity":        [0.8, 12.0, 0.5, 45.0, 1.2, 220.0, 0.3],
    "temperature":      [22,  30,   18,  28,   20,  35,    23],
    "dissolved_oxygen": [7.5, 3.2,  9.0, 5.8,  8.2, 2.5,  7.0],
    "nitrate":          [12,  48,   5,   62,   8,   78,   20],
    "fluoride":         [0.5, 1.8,  0.3, 2.4,  0.8, 0.6,  1.0],
    "iron":             [0.1, 0.8,  0.05,1.2,  0.2, 2.1,  0.15],
    "arsenic":          [2.0, 15.0, 1.0, 8.0,  4.0, 22.0, 3.0],
    "e_coli":           [0,   5,    0,   12,   0,   48,   0],
    "hardness":         [120, 450,  80,  620,  200, 380,  150],
    "sodium":           [45,  180,  30,  220,  60,  310,  80],
    "chlorides":        [80,  210,  50,  290,  100, 400,  120],
})

# ══════════════════════════════════════════════════════════════════
# CORE ANALYSIS LOGIC
# ══════════════════════════════════════════════════════════════════
def normalise_col(col: str) -> str:
    # Lowercase, strip units in brackets/parens, remove special chars
    c = col.lower().strip()
    c = re.sub(r'\(.*?\)', '', c)           # remove (mg/L), (NTU) etc
    c = re.sub(r'\[.*?\]', '', c)           # remove [mg/L] etc
    c = re.sub(r'[°µ²⁻]', '', c)           # remove special chars
    c = re.sub(r'[^a-z0-9]', '_', c)       # replace non-alphanumeric with _
    c = re.sub(r'_+', '_', c).strip('_')   # collapse underscores

    # Direct WHO param match
    if c in WHO_PARAMS:
        return c
    # Alias lookup
    if c in ALIASES:
        return ALIASES[c]
    # Fuzzy: WHO param key contained in column name or vice versa
    for pk in WHO_PARAMS:
        if pk in c or c in pk:
            return pk
    # Fuzzy: alias partial match
    for alias, target in ALIASES.items():
        if alias in c or (len(c) > 2 and c in alias):
            return target
    return c


def classify_exceedance(value, param_key: str) -> tuple:
    """Returns (status, pct_of_limit, margin)"""
    info = WHO_PARAMS[param_key]
    etype = info["exceedance_type"]
    if etype == "max":
        limit = info["max"]
        pct = (value / limit * 100) if limit else 0
        if value > limit * 1.5:
            return "Critical", pct, value - limit
        elif value > limit:
            return "Exceeded", pct, value - limit
        elif value > limit * 0.8:
            return "Watch", pct, value - limit
        else:
            return "Compliant", pct, value - limit
    elif etype == "min":
        limit = info["min"]
        pct = (value / limit * 100) if limit else 100
        if value < limit * 0.6:
            return "Critical", pct, value - limit
        elif value < limit:
            return "Exceeded", pct, value - limit
        elif value < limit * 1.15:
            return "Watch", pct, value - limit
        else:
            return "Compliant", pct, value - limit
    elif etype == "range":
        mn, mx = info["min"], info["max"]
        if value < mn or value > mx:
            dev = min(abs(value - mn), abs(value - mx))
            pct = 100 + dev / ((mx - mn) / 2) * 50
            return ("Critical" if dev > 1.5 else "Exceeded"), pct, dev
        elif value < mn + 0.3 or value > mx - 0.3:
            return "Watch", 90.0, 0
        else:
            return "Compliant", 75.0, 0
    return "Compliant", 50.0, 0


def analyse_dataframe(df: pd.DataFrame) -> dict:
    col_to_param = {}
    for col in df.columns:
        norm = normalise_col(col)
        if norm in WHO_PARAMS:
            col_to_param[col] = norm

    if not col_to_param:
        return None

    results = {}
    for col, param_key in col_to_param.items():
        info = WHO_PARAMS[param_key]
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue

        statuses = [classify_exceedance(v, param_key) for v in series]
        status_list = [s[0] for s in statuses]
        pct_list    = [s[1] for s in statuses]

        results[param_key] = {
            "col_name":    col,
            "label":       info["label"],
            "unit":        info["unit"],
            "values":      series.tolist(),
            "mean":        series.mean(),
            "median":      series.median(),
            "min_val":     series.min(),
            "max_val":     series.max(),
            "statuses":    status_list,
            "pct_of_limit":pct_list,
            "n_compliant": status_list.count("Compliant"),
            "n_watch":     status_list.count("Watch"),
            "n_exceeded":  status_list.count("Exceeded"),
            "n_critical":  status_list.count("Critical"),
            "n_total":     len(status_list),
            "who_limit":   info["who_limit"],
            "who_note":    info["who_note"],
            "source":      info["source"],
            "health":      info["health_effects"],
            "category":    info["category"],
            "mitigation":  info["mitigation"],
            "color":       info["color"],
        }

    sample_risks = []
    status_priority = {"Compliant": 0, "Watch": 1, "Exceeded": 2, "Critical": 3}
    for i in range(len(df)):
        worst = "Compliant"
        for param_key, pdata in results.items():
            if i < len(pdata["statuses"]):
                s = pdata["statuses"][i]
                if status_priority.get(s, 0) > status_priority.get(worst, 0):
                    worst = s
        sample_risks.append(worst)

    return {
        "params": results,
        "sample_risks": sample_risks,
        "n_samples": len(df),
        "col_to_param": col_to_param,
    }

# ══════════════════════════════════════════════════════════════════
# PDF BUILDER
# ══════════════════════════════════════════════════════════════════
def build_pdf(analysis: dict) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2.5*cm,
    )
    styles = getSampleStyleSheet()
    S = styles
    title_s = ParagraphStyle("T",  parent=S["Title"],   textColor=colors.HexColor("#0F6E56"), fontSize=20)
    h1_s    = ParagraphStyle("H1", parent=S["Heading1"],textColor=colors.HexColor("#0F6E56"), fontSize=14, spaceBefore=14)
    h2_s    = ParagraphStyle("H2", parent=S["Heading2"],textColor=colors.HexColor("#185FA5"), fontSize=12, spaceBefore=10)
    body_s  = ParagraphStyle("B",  parent=S["Normal"],  fontSize=9,  leading=14)
    small_s = ParagraphStyle("Sm", parent=S["Normal"],  fontSize=8,  textColor=colors.HexColor("#555555"), leading=12)
    ref_s   = ParagraphStyle("Rf", parent=S["Normal"],  fontSize=7.5,textColor=colors.HexColor("#777777"),
                              leftIndent=12, leading=11, fontName="Helvetica-Oblique")
    footer_s= ParagraphStyle("F",  parent=S["Normal"],  fontSize=7.5,textColor=colors.HexColor("#9ca3af"),
                              alignment=TA_CENTER)

    STATUS_COLORS = {
        "Compliant": colors.HexColor("#EAF3DE"),
        "Watch":     colors.HexColor("#FFF9E6"),
        "Exceeded":  colors.HexColor("#FCEBEB"),
        "Critical":  colors.HexColor("#F7C1C1"),
    }
    STATUS_TEXT = {
        "Compliant": colors.HexColor("#3B6D11"),
        "Watch":     colors.HexColor("#7D5A00"),
        "Exceeded":  colors.HexColor("#A32D2D"),
        "Critical":  colors.HexColor("#701010"),
    }

    story = []

    # ── Cover
    story += [
        Paragraph("HydroGuard AI", title_s),
        Paragraph("Water Quality Risk Assessment Report", S["Heading2"]),
        Paragraph(f"Generated: {datetime.datetime.now().strftime('%d %B %Y, %H:%M')}", small_s),
        Spacer(1, 0.2*cm),
        HRFlowable(width="100%", thickness=1.5, color=colors.HexColor("#1D9E75"), spaceAfter=10),
    ]

    # ── Executive summary
    params = analysis["params"]
    all_statuses = []
    for p in params.values():
        all_statuses += p["statuses"]
    n_total_readings = len(all_statuses)
    n_exc = sum(1 for s in all_statuses if s in ("Exceeded", "Critical"))
    n_ok  = sum(1 for s in all_statuses if s == "Compliant")

    story.append(Paragraph("Executive Summary", h1_s))
    exec_data = [
        ["Item", "Value"],
        ["Parameters analysed", str(len(params))],
        ["Total readings evaluated", str(n_total_readings)],
        ["Compliant readings", f"{n_ok} ({n_ok/n_total_readings*100:.0f}%)" if n_total_readings else "0"],
        ["Exceedances (Exceeded + Critical)", f"{n_exc} ({n_exc/n_total_readings*100:.0f}%)" if n_total_readings else "0"],
        ["Samples with Critical status", str(sum(1 for s in analysis["sample_risks"] if s == "Critical"))],
        ["Reference standard", "WHO Guidelines for Drinking-water Quality, 4th ed. (2022)"],
    ]
    t_exec = Table(exec_data, colWidths=[9*cm, 8*cm])
    t_exec.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,0), colors.HexColor("#0F6E56")),
        ("TEXTCOLOR",     (0,0),(-1,0), colors.white),
        ("FONTNAME",      (0,0),(-1,0), "Helvetica-Bold"),
        ("FONTSIZE",      (0,0),(-1,-1), 9),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.HexColor("#F9FAFB"), colors.white]),
        ("GRID",          (0,0),(-1,-1), 0.4, colors.HexColor("#e5e7eb")),
        ("LEFTPADDING",   (0,0),(-1,-1), 8),
        ("BOTTOMPADDING", (0,0),(-1,-1), 5),
    ]))
    story += [t_exec, Spacer(1, 0.5*cm)]

    # ── Overview chart
    story.append(Paragraph("Parameter Compliance Overview", h1_s))
    labels_chart, exc_vals, comp_vals = [], [], []
    for pk, pd_ in params.items():
        n = pd_["n_total"]
        if n == 0:
            continue
        labels_chart.append(pd_["label"])
        exc_vals.append((pd_["n_exceeded"] + pd_["n_critical"]) / n * 100)
        comp_vals.append(pd_["n_compliant"] / n * 100)

    if labels_chart:
        y_pos = np.arange(len(labels_chart))
        fig, ax = plt.subplots(figsize=(10, max(3, len(labels_chart)*0.5 + 1)))
        ax.barh(y_pos, comp_vals, color="#EAF3DE", edgecolor="#3B6D11", linewidth=0.7, label="Compliant %")
        ax.barh(y_pos, exc_vals,  color="#FCEBEB", edgecolor="#A32D2D", linewidth=0.7,
                left=comp_vals, label="Exceeded %")
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels_chart, fontsize=8)
        ax.set_xlabel("% of samples", fontsize=9)
        ax.set_xlim(0, 100)
        ax.legend(fontsize=8)
        ax.spines[["top","right"]].set_visible(False)
        plt.tight_layout()
        cbuf = io.BytesIO()
        fig.savefig(cbuf, format="png", dpi=130, bbox_inches="tight")
        plt.close(fig)
        cbuf.seek(0)
        story += [
            RLImage(cbuf, width=15*cm, height=max(5*cm, len(labels_chart)*0.55*cm + 2*cm)),
            Spacer(1, 0.5*cm),
        ]

    # ── Per-parameter sections
    story.append(Paragraph("Detailed Parameter Analysis", h1_s))

    for pk, pd_ in params.items():
        n = pd_["n_total"]
        if n == 0:
            continue

        if pd_["n_critical"] > 0:
            overall = "Critical"
        elif pd_["n_exceeded"] > 0:
            overall = "Exceeded"
        elif pd_["n_watch"] > 0:
            overall = "Watch"
        else:
            overall = "Compliant"

        unit_str = f" {pd_['unit']}" if pd_['unit'] else ""
        block = []
        block.append(Paragraph(f"{pd_['label']}  [{pd_['category']}]", h2_s))

        badge_bg   = STATUS_COLORS.get(overall, colors.white)
        badge_text = STATUS_TEXT.get(overall, colors.black)
        badge_table = Table([[overall]], colWidths=[3*cm])
        badge_table.setStyle(TableStyle([
            ("BACKGROUND",   (0,0),(-1,-1), badge_bg),
            ("TEXTCOLOR",    (0,0),(-1,-1), badge_text),
            ("FONTNAME",     (0,0),(-1,-1), "Helvetica-Bold"),
            ("FONTSIZE",     (0,0),(-1,-1), 9),
            ("ALIGN",        (0,0),(-1,-1), "CENTER"),
            ("TOPPADDING",   (0,0),(-1,-1), 4),
            ("BOTTOMPADDING",(0,0),(-1,-1), 4),
        ]))
        block.append(badge_table)
        block.append(Spacer(1, 0.2*cm))

        stats_data = [
            ["WHO Guideline", pd_["who_limit"]],
            ["Mean", f"{pd_['mean']:.3g}{unit_str}"],
            ["Median", f"{pd_['median']:.3g}{unit_str}"],
            ["Min / Max", f"{pd_['min_val']:.3g} / {pd_['max_val']:.3g}{unit_str}"],
            ["Compliant samples", f"{pd_['n_compliant']}/{n} ({pd_['n_compliant']/n*100:.0f}%)"],
            ["Exceedances", f"{pd_['n_exceeded'] + pd_['n_critical']}/{n} ({(pd_['n_exceeded']+pd_['n_critical'])/n*100:.0f}%)"],
        ]
        t_stats = Table(stats_data, colWidths=[5*cm, 12*cm])
        t_stats.setStyle(TableStyle([
            ("FONTSIZE",      (0,0),(-1,-1), 8),
            ("FONTNAME",      (0,0),(0,-1),  "Helvetica-Bold"),
            ("ROWBACKGROUNDS",(0,0),(-1,-1),[colors.HexColor("#F0F9FF"), colors.white]),
            ("GRID",          (0,0),(-1,-1), 0.3, colors.HexColor("#e5e7eb")),
            ("LEFTPADDING",   (0,0),(-1,-1), 6),
            ("BOTTOMPADDING", (0,0),(-1,-1), 4),
        ]))
        block.append(t_stats)
        block.append(Spacer(1, 0.2*cm))
        block.append(Paragraph(f"<b>WHO Note:</b> {pd_['who_note']}", small_s))
        block.append(Paragraph(f"<i>Source: {pd_['source']}</i>", ref_s))
        block.append(Spacer(1, 0.15*cm))
        block.append(Paragraph(f"<b>Health Effects:</b> {pd_['health']}", small_s))
        block.append(Spacer(1, 0.2*cm))

        if pd_["n_exceeded"] + pd_["n_critical"] + pd_["n_watch"] > 0:
            block.append(Paragraph("Recommended Mitigation Strategies", h2_s))
            for i, m in enumerate(pd_["mitigation"], 1):
                block.append(Paragraph(f"<b>{i}. {m['strategy']}</b>", body_s))
                block.append(Paragraph(m["action"], body_s))
                block.append(Paragraph(f"Reference: {m['reference']}", ref_s))
                block.append(Spacer(1, 0.15*cm))

        block.append(HRFlowable(width="100%", thickness=0.4, color=colors.HexColor("#e5e7eb"), spaceAfter=6))
        story.append(KeepTogether(block[:6]))
        story += block[6:]

    # ── References
    story += [
        Spacer(1, 0.5*cm),
        HRFlowable(width="100%", thickness=0.8, color=colors.HexColor("#1D9E75")),
        Paragraph("Key References", h1_s),
        Paragraph("WHO (2022). Guidelines for Drinking-water Quality: Fourth Edition Incorporating the First and Second Addenda. Geneva: World Health Organization. ISBN 978-92-4-004506-4.", ref_s),
        Paragraph("Crittenden, J.C. et al. (2012). MWH's Water Treatment: Principles and Design, 3rd Edition. John Wiley & Sons.", ref_s),
        Paragraph("Tchobanoglous, G. et al. (2014). Wastewater Engineering: Treatment and Resource Recovery, 5th Edition. McGraw-Hill.", ref_s),
        Paragraph("UNEP (2016). A Snapshot of the World's Water Quality: Advancing a Global Assessment. Nairobi: UNEP.", ref_s),
        Paragraph("EU Water Framework Directive 2000/60/EC. Official Journal of the European Communities.", ref_s),
        Paragraph("US EPA (2012). 2012 Edition of the Drinking Water Standards and Health Advisories. EPA 822-S-12-001.", ref_s),
        Paragraph("Kadlec, R.H. & Wallace, S.D. (2009). Treatment Wetlands, 2nd Edition. CRC Press.", ref_s),
        Spacer(1, 0.5*cm),
        HRFlowable(width="100%", thickness=0.4, color=colors.HexColor("#e5e7eb")),
        Paragraph(
            "HydroGuard AI - AI-powered water risk intelligence platform - "
            "This report compares measured values against WHO (2022) Guidelines for Drinking-water Quality. "
            "Results are for guidance only and must be interpreted by a qualified water quality professional.",
            footer_s,
        ),
    ]

    doc.build(story)
    buf.seek(0)
    return buf.read()

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
STEP_PROGRESS = {"upload": 0.15, "preview": 0.45, "analyse": 0.80, "report": 1.0}
STEP_LABELS   = {
    "upload":  "Step 1 of 4 — Upload data",
    "preview": "Step 2 of 4 — Preview & validate",
    "analyse": "Step 3 of 4 — WHO comparison & analysis",
    "report":  "Step 4 of 4 — Detailed report",
}

def step_pill(label):
    st.markdown(f'<div class="step-pill">{label}</div>', unsafe_allow_html=True)

def status_badge(status):
    bg = BG_MAP.get(status, "#f3f4f6")
    fg = FG_MAP.get(status, "#374151")
    return f'<span style="background:{bg};color:{fg};padding:2px 12px;border-radius:20px;font-weight:700;font-size:0.82rem;">{status}</span>'

# ══════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════
c1, c2 = st.columns([1, 2])
with c1:
    st.markdown('<p class="hg-title">💧 HydroGuard AI</p>', unsafe_allow_html=True)
    st.markdown('<p class="hg-sub">WHO guidelines · Water risk intelligence</p>', unsafe_allow_html=True)
with c2:
    step = st.session_state.step
    st.caption(STEP_LABELS[step])
    st.progress(STEP_PROGRESS[step])

st.divider()

# ══════════════════════════════════════════════════════════════════
# STEP 1 — UPLOAD
# ══════════════════════════════════════════════════════════════════
if st.session_state.step == "upload":

    step_pill("Step 1 of 4 · Upload your water quality data")
    st.markdown("### Welcome to HydroGuard AI")
    st.markdown(
        "Upload a CSV or Excel file containing your water quality measurements. "
        "The app automatically detects which parameters you have and compares each one "
        "against **WHO (2022) drinking water guidelines**, providing compliance status, "
        "health effect information, and evidence-based mitigation strategies."
    )

    col_up, col_samp = st.columns([1.3, 1], gap="large")

    with col_up:
        st.markdown("#### Upload your file")
        st.caption(
            "Accepted formats: CSV (.csv), Excel (.xlsx, .xls, .xlsm, .xlsb) or ODS (.ods). "
            "Column names can be in any language — common names and abbreviations are auto-detected."
        )
        uploaded = st.file_uploader(
            "Drop file here or click Browse",
            type=["csv", "xlsx", "xls", "xlsm", "xlsb", "ods"],
            label_visibility="collapsed",
        )
        if uploaded:
            try:
                ext = uploaded.name.rsplit(".", 1)[-1].lower()
                if ext in ("xlsx", "xls", "xlsm", "xlsb", "ods"):
                    df = pd.read_excel(uploaded, engine=None)
                else:
                    df = pd.read_csv(uploaded, encoding_errors="replace")

                if df.empty:
                    st.error("The uploaded file appears to be empty.")
                else:
                    st.session_state.df_raw = df
                    st.session_state.step   = "preview"
                    st.success(f"Loaded **{uploaded.name}** — {len(df)} rows, {len(df.columns)} columns.")
                    st.rerun()
            except Exception as e:
                st.error(f"Could not read file: {e}")

    with col_samp:
        st.markdown("#### Or try the sample dataset")
        st.caption("7 samples across 12 parameters — includes deliberate exceedances for demonstration.")
        st.dataframe(SAMPLE.head(4), use_container_width=True, hide_index=True)
        if st.button("Use sample data  →", use_container_width=True, type="primary"):
            st.session_state.df_raw = SAMPLE.copy()
            st.session_state.step   = "preview"
            st.rerun()
        st.download_button(
            "⬇  Download sample CSV", SAMPLE.to_csv(index=False),
            "hydroguard_sample.csv", "text/csv", use_container_width=True,
        )

    st.markdown("---")
    with st.expander("Which parameters does HydroGuard AI recognise?"):
        rows = []
        for pk, info in WHO_PARAMS.items():
            rows.append({
                "Parameter": info["label"], "Unit": info["unit"],
                "WHO Guideline": info["who_limit"], "Category": info["category"],
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.caption(
            "Column names in your file are automatically matched — e.g. 'temp', 'Temp_C', "
            "'temperature' all map to Temperature. 'chlorides' maps to Chloride. "
            "'sodium' / 'Na' map to Sodium."
        )

# ══════════════════════════════════════════════════════════════════
# STEP 2 — PREVIEW & DETECT
# ══════════════════════════════════════════════════════════════════
elif st.session_state.step == "preview":

    df = st.session_state.df_raw
    step_pill("Step 2 of 4 · Preview & validate")
    st.markdown(f"### Data preview — {len(df)} rows, {len(df.columns)} columns")
    st.dataframe(df.head(8), use_container_width=True, hide_index=True)

    detected   = {}
    undetected = []
    for col in df.columns:
        norm = normalise_col(col)
        if norm in WHO_PARAMS:
            detected[col] = norm
        else:
            undetected.append(col)

    st.markdown("---")
    st.markdown("#### Auto-detected parameters")
    if detected:
        det_rows = [
            {
                "Your column": col,
                "Matched parameter": WHO_PARAMS[norm]["label"],
                "WHO guideline": WHO_PARAMS[norm]["who_limit"],
            }
            for col, norm in detected.items()
        ]
        st.dataframe(pd.DataFrame(det_rows), use_container_width=True, hide_index=True)
    else:
        st.warning("No recognised water quality parameters found. Please check your column names.")

    if undetected:
        with st.expander(f"{len(undetected)} column(s) not matched — click to review"):
            debug_rows = [
                {
                    "Your column": col,
                    "Normalised to": normalise_col(col),
                    "Status": "No WHO parameter match — add to ALIASES if needed",
                }
                for col in undetected
            ]
            st.dataframe(pd.DataFrame(debug_rows), use_container_width=True, hide_index=True)
            st.caption(
                "The 'Normalised to' column shows how your column name was interpreted. "
                "If it's close to a parameter name, add it to the ALIASES dict in the code."
            )

    if not detected:
        if st.button("← Back", use_container_width=True):
            st.session_state.step = "upload"
            st.rerun()
    else:
        b1, b2 = st.columns([1, 5])
        with b1:
            if st.button("← Back", use_container_width=True):
                st.session_state.step = "upload"
                st.rerun()
        with b2:
            if st.button(
                f"Analyse {len(detected)} parameter(s) against WHO limits  →",
                use_container_width=True, type="primary",
            ):
                with st.spinner("Comparing against WHO guidelines..."):
                    analysis = analyse_dataframe(df)
                if analysis:
                    st.session_state.analysis = analysis
                    # Clear cached PDF when analysis changes
                    st.session_state.pdf_bytes = None
                    st.session_state.pdf_analysis_id = None
                    st.session_state.step = "analyse"
                    st.rerun()
                else:
                    st.error("Analysis failed — no numeric data found in detected columns.")

# ══════════════════════════════════════════════════════════════════
# STEP 3 — WHO COMPARISON RESULTS
# ══════════════════════════════════════════════════════════════════
elif st.session_state.step == "analyse":

    analysis = st.session_state.analysis
    params   = analysis["params"]

    step_pill("Step 3 of 4 · WHO comparison results")
    st.markdown(f"### Results — {len(params)} parameters, {analysis['n_samples']} samples")

    all_statuses = []
    for p in params.values():
        all_statuses += p["statuses"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Compliant readings", sum(1 for s in all_statuses if s == "Compliant"))
    c2.metric("Watch",              sum(1 for s in all_statuses if s == "Watch"))
    c3.metric("Exceeded",           sum(1 for s in all_statuses if s == "Exceeded"))
    c4.metric("Critical",           sum(1 for s in all_statuses if s == "Critical"))

    st.markdown("---")
    st.markdown("**Parameter compliance overview**")
    labels_ch, exc_v, comp_v, watch_v = [], [], [], []
    for pk, pd_ in params.items():
        n = pd_["n_total"]
        if n == 0:
            continue
        labels_ch.append(pd_["label"])
        comp_v.append(pd_["n_compliant"] / n * 100)
        watch_v.append(pd_["n_watch"] / n * 100)
        exc_v.append((pd_["n_exceeded"] + pd_["n_critical"]) / n * 100)

    fig0, ax0 = plt.subplots(figsize=(10, max(3, len(labels_ch)*0.55 + 1)))
    y = np.arange(len(labels_ch))
    ax0.barh(y, comp_v,  color="#EAF3DE", edgecolor="#3B6D11", linewidth=0.6, label="Compliant")
    ax0.barh(y, watch_v, color="#FFF9E6", edgecolor="#BA7517", linewidth=0.6, label="Watch",    left=comp_v)
    ax0.barh(y, exc_v,   color="#FCEBEB", edgecolor="#A32D2D", linewidth=0.6, label="Exceeded",
             left=[c+w for c, w in zip(comp_v, watch_v)])
    ax0.set_yticks(y)
    ax0.set_yticklabels(labels_ch, fontsize=9)
    ax0.set_xlim(0, 100)
    ax0.set_xlabel("% of samples", fontsize=9)
    ax0.legend(fontsize=9, loc="lower right")
    ax0.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    st.pyplot(fig0, use_container_width=True)
    plt.close(fig0)

    st.markdown("---")
    st.markdown("### Parameter-by-parameter analysis")

    for pk, pd_ in params.items():
        n = pd_["n_total"]
        if n == 0:
            continue

        overall = (
            "Critical" if pd_["n_critical"] > 0 else
            "Exceeded" if pd_["n_exceeded"] > 0 else
            "Watch"    if pd_["n_watch"] > 0 else
            "Compliant"
        )
        unit_str = f" {pd_['unit']}" if pd_['unit'] else ""

        with st.expander(
            f"{pd_['label']}  —  {status_badge(overall)}  "
            f"({pd_['n_compliant']}/{n} samples compliant)",
            expanded=(overall in ("Critical", "Exceeded")),
        ):
            col_a, col_b = st.columns([1, 1.6])

            with col_a:
                st.markdown(f"**WHO guideline:** {pd_['who_limit']}")
                st.markdown(f"**Mean:** {pd_['mean']:.3g}{unit_str}  |  **Median:** {pd_['median']:.3g}{unit_str}")
                st.markdown(f"**Range:** {pd_['min_val']:.3g} – {pd_['max_val']:.3g}{unit_str}")
                st.markdown("")
                counts_disp = pd.DataFrame({
                    "Status": ["Compliant", "Watch", "Exceeded", "Critical"],
                    "Count": [pd_["n_compliant"], pd_["n_watch"], pd_["n_exceeded"], pd_["n_critical"]],
                    "% Samples": [
                        f"{v/n*100:.0f}%"
                        for v in [pd_["n_compliant"], pd_["n_watch"], pd_["n_exceeded"], pd_["n_critical"]]
                    ],
                })
                st.dataframe(counts_disp, use_container_width=True, hide_index=True)

            with col_b:
                info = WHO_PARAMS[pk]
                fig1, ax1 = plt.subplots(figsize=(5, 2.8))
                ax1.hist(pd_["values"], bins=min(20, n), color=pd_["color"],
                         alpha=0.7, edgecolor="white", linewidth=0.5)
                if info["exceedance_type"] == "max" and info["max"] is not None:
                    ax1.axvline(info["max"], color="#E24B4A", linewidth=1.5,
                                linestyle="--", label=f"WHO limit ({info['max']})")
                elif info["exceedance_type"] == "min" and info["min"] is not None:
                    ax1.axvline(info["min"], color="#E24B4A", linewidth=1.5,
                                linestyle="--", label=f"WHO min ({info['min']})")
                elif info["exceedance_type"] == "range":
                    ax1.axvline(info["min"], color="#E24B4A", linewidth=1.5,
                                linestyle="--", label=f"Min ({info['min']})")
                    ax1.axvline(info["max"], color="#E24B4A", linewidth=1.5,
                                linestyle="--", label=f"Max ({info['max']})")
                ax1.set_xlabel(f"{pd_['label']}{unit_str}", fontsize=9)
                ax1.set_ylabel("Samples", fontsize=9)
                ax1.legend(fontsize=8)
                ax1.spines[["top","right"]].set_visible(False)
                plt.tight_layout()
                st.pyplot(fig1, use_container_width=True)
                plt.close(fig1)

            st.markdown(
                f'<div class="who-box">📋 <b>WHO note:</b> {pd_["who_note"]}<br>'
                f'<small><i>Source: {pd_["source"]}</i></small></div>',
                unsafe_allow_html=True,
            )
            st.markdown(f"**Health effects:** {pd_['health']}")

            if pd_["n_exceeded"] + pd_["n_critical"] + pd_["n_watch"] > 0:
                st.markdown("---")
                st.markdown("**Recommended mitigation strategies**")
                for i, m in enumerate(pd_["mitigation"], 1):
                    st.markdown(f"**{i}. {m['strategy']}**")
                    st.markdown(m["action"])
                    st.markdown(
                        f'<div class="ref-box">📚 {m["reference"]}</div>',
                        unsafe_allow_html=True,
                    )

    st.markdown("---")
    st.markdown("### Sample-level risk summary")
    df_raw = st.session_state.df_raw
    summary_df = df_raw.copy()
    summary_df.insert(0, "Sample", range(1, len(df_raw)+1))
    summary_df["Overall risk"] = analysis["sample_risks"]

    def colour_overall(val):
        return f"background-color:{BG_MAP.get(val,'')};color:{FG_MAP.get(val,'')}"

    styled_sum = summary_df.style.applymap(colour_overall, subset=["Overall risk"])
    st.dataframe(styled_sum, use_container_width=True, hide_index=True)

    st.markdown("---")
    b1, b2 = st.columns([1, 5])
    with b1:
        if st.button("← Back", use_container_width=True):
            st.session_state.step = "preview"
            st.rerun()
    with b2:
        if st.button("Generate detailed report  →", use_container_width=True, type="primary"):
            st.session_state.step = "report"
            st.rerun()

# ══════════════════════════════════════════════════════════════════
# STEP 4 — REPORT  (PDF cached in session_state to survive reruns)
# ══════════════════════════════════════════════════════════════════
elif st.session_state.step == "report":

    analysis = st.session_state.analysis
    df_raw   = st.session_state.df_raw

    step_pill("Step 4 of 4 · Download report")
    st.markdown("### Your report is ready")
    st.success(
        f"Analysis of **{len(analysis['params'])} parameters** across "
        f"**{analysis['n_samples']} samples** is complete. "
        "Download your results below."
    )

    all_statuses = [s for p in analysis["params"].values() for s in p["statuses"]]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Compliant", sum(1 for s in all_statuses if s == "Compliant"))
    c2.metric("Watch",     sum(1 for s in all_statuses if s == "Watch"))
    c3.metric("Exceeded",  sum(1 for s in all_statuses if s == "Exceeded"))
    c4.metric("Critical",  sum(1 for s in all_statuses if s == "Critical"))

    st.markdown("---")
    d1, d2 = st.columns(2)

    with d1:
        st.markdown("#### CSV results")
        st.caption("Sample-level results with WHO compliance status for each detected parameter.")
        out_df = df_raw.copy()
        out_df["overall_risk"] = analysis["sample_risks"]
        for pk, pd_ in analysis["params"].items():
            label = pd_["label"].replace(" ", "_").lower()
            if len(pd_["statuses"]) == len(out_df):
                out_df[f"{label}_status"] = pd_["statuses"]
        st.download_button(
            "⬇  Download results CSV",
            data=out_df.to_csv(index=False),
            file_name=f"hydroguard_results_{datetime.date.today()}.csv",
            mime="text/csv",
            use_container_width=True,
            type="primary",
        )

    with d2:
        st.markdown("#### Full PDF report")
        st.caption(
            "Includes: executive summary, per-parameter WHO compliance analysis, "
            "health effects, mitigation strategies with literature references, and charts."
        )
        # Generate PDF once and cache — avoids regenerating on every Streamlit rerun
        if (
            st.session_state.pdf_bytes is None
            or st.session_state.pdf_analysis_id != id(analysis)
        ):
            with st.spinner("Building PDF report..."):
                try:
                    st.session_state.pdf_bytes = build_pdf(analysis)
                    st.session_state.pdf_analysis_id = id(analysis)
                except Exception as e:
                    st.error(f"PDF generation failed: {e}")
                    st.session_state.pdf_bytes = None

        if st.session_state.pdf_bytes:
            st.download_button(
                "⬇  Download PDF report",
                data=st.session_state.pdf_bytes,
                file_name=f"hydroguard_report_{datetime.date.today()}.pdf",
                mime="application/pdf",
                use_container_width=True,
                type="primary",
            )

    st.markdown("---")
    with st.expander("WHO guidelines reference table"):
        ref_rows = []
        for pk, info in WHO_PARAMS.items():
            ref_rows.append({
                "Parameter": info["label"], "Unit": info["unit"],
                "WHO Guideline": info["who_limit"], "Category": info["category"],
                "Source": info["source"],
            })
        st.dataframe(pd.DataFrame(ref_rows), use_container_width=True, hide_index=True)

    st.markdown("---")
    b1, b2 = st.columns([1, 5])
    with b1:
        if st.button("← Back to results", use_container_width=True):
            st.session_state.step = "analyse"
            st.rerun()
    with b2:
        if st.button("Start new analysis  →", use_container_width=True):
            for key in ["step", "df_raw", "analysis", "pdf_bytes", "pdf_analysis_id"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()