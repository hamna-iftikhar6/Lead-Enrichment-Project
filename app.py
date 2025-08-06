import streamlit as st
import pandas as pd
from datetime import datetime
import os

# Page configuration
st.set_page_config(
    page_title="Bayesian Lead Enrichment Engine",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .phase-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
    }
    .status-box {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .status-pending {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
    }
    .status-complete {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .status-error {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
    }
    .info-box {
        background-color: #e3f2fd;
        border: 1px solid #2196f3;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3e0;
        border: 1px solid #ff9800;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def main():
    # Main header
    st.markdown('<h1 class="main-header">🧠 Bayesian Lead Enrichment Engine</h1>', unsafe_allow_html=True)
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Lead Type Selection
        lead_type = st.selectbox(
            "Lead Type",
            ["Individual", "Business"],
            help="Select the type of leads to process"
        )
        
        st.markdown("---")
        
        # Enrichment Sources (placeholder for Phase 3)
        st.subheader("🔍 Enrichment Sources")
        fps_enabled = st.checkbox("FPS", value=True, disabled=True)
        spokeo_enabled = st.checkbox("Spokeo", value=True, disabled=True)
        pdl_enabled = st.checkbox("PDL", value=True, disabled=True)
        sos_enabled = st.checkbox("SOS Matching", value=(lead_type == "Business"), disabled=True)
        
        st.markdown("---")
        
        # Confidence Thresholds
        st.subheader("🎯 Confidence Thresholds")
        auto_accept_threshold = st.slider(
            "Auto-Accept (%)", 
            min_value=50, max_value=90, value=70, step=5,
            help="Leads with confidence ≥ this threshold are auto-accepted"
        )
        manual_review_min = st.slider(
            "Manual Review Min (%)", 
            min_value=20, max_value=60, value=40, step=5,
            help="Minimum confidence for manual review"
        )
        
        st.markdown("---")
        
        # System Status
        st.subheader("📊 System Status")
        st.info("🟢 All systems operational")
        st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

    # Main content area - App Information and Instructions
    st.markdown("## 📋 How This App Works")
    
    # App Overview
    with st.expander("🎯 App Overview", expanded=True):
        st.markdown("""
        **The Bayesian Lead Enrichment Engine** is an intelligent pipeline that processes raw lead data and enriches it with third-party contact information to improve lead quality and conversion rates.
        
        ### 🎯 What This App Does:
        1. **Ingests** raw lead files (individuals and businesses)
        2. **Enriches** data with third-party sources (FPS, Spokeo, PDL)
        3. **Computes** Bayesian confidence scores for contact accuracy
        4. **Decides** automatically based on confidence thresholds
        5. **Pushes** qualified leads to your CRM (Pipedrive)
        """)
    
    # File Upload Instructions
    with st.expander("📁 What Files to Upload", expanded=True):
        st.markdown("""
        ### 📤 Supported File Formats:
        - **CSV files only** (.csv extension)
        - Maximum file size: 50MB
        - Encoding: UTF-8 recommended
        
        ### ⚠️ Important Notes:
        - Ensure column headers match exactly
        - Company names should be complete and accurate
        """)
    
    # Process Flow
    with st.expander("🔄 How the Process Works", expanded=True):
        st.markdown("""
        ### 🔄 Processing Pipeline:
        
        1. **📁 File Upload & Validation**
           - Upload your CSV file
           - System validates file format and structure
           - Preview data before processing
           
        2. **🔍 Data Enrichment**
           - **Individuals:** Online enrichment only (FPS, Spokeo, PDL)
           - **Businesses:** SOS matching + online enrichment
           - Multiple data sources cross-validate information
           
        3. **🧮 Bayesian Scoring**
           - Calculates confidence scores for each contact method
           - Uses log-probability calculations
           - Considers data source reliability and consistency
           
        4. **🎯 Decision Logic**
           - **≥70% confidence:** Auto-accept → CRM
           - **40-70% confidence:** Manual review queue
           - **<40% confidence:** Reject
           
        5. **📤 CRM Integration**
           - Pushes accepted leads to Pipedrive
           - Maintains audit trail
           - Handles duplicates and conflicts
        """)
    
    # What You'll Get
    with st.expander("📊 What You'll Receive", expanded=True):
        st.markdown("""
        ### 📈 Output Results:
        
        **1. Enriched Contact Data:**
        - Original + enriched email addresses
        - Original + enriched phone numbers
        - Company information (for individuals)
        - Industry classification (for businesses)
        
        **2. Confidence Scores:**
        - Email confidence percentage
        - Phone confidence percentage
        - Overall lead confidence score
        - Data source reliability indicators
        
        **3. Decision Categories:**
        - **Auto-Accepted:** High-confidence leads ready for CRM
        - **Manual Review:** Medium-confidence leads needing human review
        - **Rejected:** Low-confidence leads filtered out
        
        **4. Detailed Reports:**
        - Processing summary statistics
        - Enrichment success rates
        - Confidence score distribution
        - Data source performance metrics
        """)
    
    # Usage Instructions
    with st.expander("📖 Step-by-Step Instructions", expanded=True):
        st.markdown("""
        ### 🚀 Getting Started:
        
        **Step 1: Prepare Your Data**
        - Export your leads to CSV format
        - Ensure required columns are present
        - Clean and validate your data
        
        **Step 2: Configure Settings**
        - Select lead type (Individual/Business)
        - Choose enrichment sources
        - Set confidence thresholds
        - Configure CRM settings
        
        **Step 3: Upload & Process**
        - Upload your CSV file
        - Review data preview
        - Start enrichment process
        - Monitor progress
        
        **Step 4: Review Results**
        - Check confidence scores
        - Review auto-accepted leads
        - Manually review medium-confidence leads
        - Export results to CRM
        
        **Step 5: Monitor & Optimize**
        - Track processing statistics
        - Analyze confidence distributions
        - Adjust thresholds as needed
        - Monitor CRM integration
        """)
    
    # Technical Details
    with st.expander("🔧 Technical Details", expanded=True):
        st.markdown("""
        ### 🛠️ Technical Architecture:
        
        **Data Sources:**
        - **FPS (Fast People Search):** Primary contact validation
        - **Spokeo:** People search and verification
        - **PDL (People Data Labs):** Professional contact enrichment
        - **SOS (Secretary of State):** Business entity verification
        
        **Scoring Algorithm:**
        - Bayesian log-probability calculations
        - Multi-source data fusion
        - Confidence interval estimation
        - Reliability weighting by source
        
        **Performance Features:**
        - Asynchronous processing
        - Batch processing capabilities
        - Real-time progress tracking
        - Error handling and retry logic
        
        **Security & Compliance:**
        - Data encryption in transit
        - Secure API connections
        - DNC (Do Not Call) filtering
        - GDPR compliance measures
        """)
    
    # Troubleshooting
    with st.expander("🔧 Troubleshooting", expanded=True):
        st.markdown("""
        ### ❗ Common Issues & Solutions:
        
        **File Upload Issues:**
        - **Problem:** "Invalid file format"
        - **Solution:** Ensure file is CSV format with .csv extension
        
        - **Problem:** "Missing required columns"
        - **Solution:** Check column headers match exactly
        
        **Processing Issues:**
        - **Problem:** "Enrichment failed"
        - **Solution:** Check internet connection and API keys
        
        - **Problem:** "Low confidence scores"
        - **Solution:** Verify data quality and completeness
        
        **CRM Integration Issues:**
        - **Problem:** "CRM connection failed"
        - **Solution:** Verify API keys and permissions
        
        ### 📞 Support:
        - Check the documentation for detailed guides
        - Review error logs for specific issues
        - Contact support for technical assistance
        """)

    # Footer with progress tracking
    st.markdown("---")
    st.subheader("📋 Phase Progress")
    
    phases = [
        ("Phase 1", "Initial Streamlit Skeleton", "✅ Complete"),
        ("Phase 2", "Upload + Preview UI", "⏳ Pending"),
        ("Phase 3", "Sidebar Parameters", "⏳ Pending"),
        ("Phase 4", "Web Enrichment", "⏳ Pending"),
        ("Phase 5", "SOS Matching", "⏳ Pending"),
        ("Phase 6", "Comparative Table", "⏳ Pending"),
        ("Phase 7", "Bayesian Scorer", "⏳ Pending"),
        ("Phase 8", "Threshold Gate + CRM", "⏳ Pending"),
        ("Phase 9", "Retry/Manual Loop", "⏳ Pending"),
        ("Phase 10", "Progress Display", "⏳ Pending"),
        ("Phase 11", "Testing & Polish", "⏳ Pending")
    ]
    
    for phase_num, phase_name, status in phases:
        st.write(f"**{phase_num}:** {phase_name} - {status}")

if __name__ == "__main__":
    main() 