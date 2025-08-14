import streamlit as st
import pandas as pd
from pathlib import Path
import json

# Page configuration
st.set_page_config(
    page_title="Parameter Configuration - Lead Enrichment",
    page_icon="⚙️",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .parameter-section {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
    }
    .config-card {
        background-color: #ffffff;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .source-item {
        background-color: #e9ecef;
        border-radius: 5px;
        padding: 10px;
        margin: 5px 0;
    }
    .stSlider > div > div > div > div {
        background-color: #007bff;
    }
</style>
""", unsafe_allow_html=True)

def load_config():
    """Load existing configuration if available"""
    config_file = Path("config/enrichment_config.json")
    if config_file.exists():
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            st.warning(f"Could not load existing config: {e}")
    return {}

def save_config(config):
    """Save configuration to file"""
    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)
    
    config_file = config_dir / "enrichment_config.json"
    try:
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        st.error(f"Error saving configuration: {e}")
        return False

def main():
    st.title("⚙️ Parameter Configuration")
    st.markdown("**Phase 3:** Configure Enrichment Parameters")
    
    # Load existing configuration
    existing_config = load_config()
    
    # Sidebar for main parameters
    with st.sidebar:
        st.header("🎯 Lead Type Configuration")
        
        # Lead type toggle
        lead_type = st.selectbox(
            "Lead Type",
            ["Individual", "Business", "Mixed"],
            index=0 if existing_config.get('lead_type') is None else 
                  ["Individual", "Business", "Mixed"].index(existing_config.get('lead_type', 'Individual')),
            help="Select the type of leads you're processing"
        )
        
        st.markdown("---")
        
        st.header("📊 Processing Limits")
        
        # Lead count slider
        max_leads = st.slider(
            "Maximum Leads to Process",
            min_value=10,
            max_value=10000,
            value=existing_config.get('max_leads', 1000),
            step=10,
            help="Maximum number of leads to process in this batch"
        )
        
        # Batch size for processing
        batch_size = st.slider(
            "Batch Size",
            min_value=5,
            max_value=100,
            value=existing_config.get('batch_size', 20),
            step=5,
            help="Number of leads to process in each batch"
        )
        
        st.markdown("---")
        
        st.header("⏱️ Timing Configuration")
        
        # Delay between requests
        min_delay = st.slider(
            "Minimum Delay (seconds)",
            min_value=1,
            max_value=10,
            value=existing_config.get('min_delay', 3),
            help="Minimum delay between requests to avoid detection"
        )
        
        max_delay = st.slider(
            "Maximum Delay (seconds)",
            min_value=5,
            max_value=20,
            value=existing_config.get('max_delay', 8),
            help="Maximum delay between requests"
        )
        
        # Retry configuration
        max_retries = st.slider(
            "Maximum Retries",
            min_value=1,
            max_value=5,
            value=existing_config.get('max_retries', 3),
            help="Number of retry attempts for failed requests"
        )
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<div class="parameter-section">', unsafe_allow_html=True)
        st.subheader("🔍 Enrichment Sources")
        
        # Available enrichment sources
        available_sources = {
            "FastPeopleSearch": {
                "description": "Phone numbers, emails, age, addresses, relatives",
                "enabled": existing_config.get('sources', {}).get('FastPeopleSearch', True),
                "priority": 1
            },
            "Spokeo": {
                "description": "Comprehensive people search and background info",
                "enabled": existing_config.get('sources', {}).get('Spokeo', False),
                "priority": 2
            },
            "PeopleDataLabs": {
                "description": "Professional contact data and company info",
                "enabled": existing_config.get('sources', {}).get('PeopleDataLabs', False),
                "priority": 3
            },
            "SOS Match": {
                "description": " SOS business match",
                "enabled": existing_config.get('sources', {}).get('SOS Match chunks', False),
                "priority": 4
            }
        }
        
        # Source selection
        selected_sources = {}
        for source_name, source_info in available_sources.items():
            with st.expander(f"📡 {source_name}", expanded=True):
                st.write(f"**Description:** {source_info['description']}")
                
                enabled = st.checkbox(
                    f"Enable {source_name}",
                    value=source_info['enabled'],
                    key=f"source_{source_name}"
                )
                
                if enabled:
                    priority = st.slider(
                        f"Priority for {source_name}",
                        min_value=1,
                        max_value=10,
                        value=source_info['priority'],
                        key=f"priority_{source_name}"
                    )
                    
                    # Source-specific settings
                    if source_name == "FastPeopleSearch":
                        fps_settings = {
                            "extract_phones": st.checkbox("Extract phone numbers", True, key="fps_phones"),
                            "extract_emails": st.checkbox("Extract email addresses", True, key="fps_emails"),
                            "extract_age": st.checkbox("Extract age information", True, key="fps_age"),
                            "extract_relatives": st.checkbox("Extract relatives", True, key="fps_relatives"),
                            "extract_addresses": st.checkbox("Extract address history", True, key="fps_addresses")
                        }
                        selected_sources[source_name] = {
                            "enabled": enabled,
                            "priority": priority,
                            "settings": fps_settings
                        }
                    else:
                        selected_sources[source_name] = {
                            "enabled": enabled,
                            "priority": priority,
                            "settings": {}
                        }
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Data quality settings
        st.markdown('<div class="parameter-section">', unsafe_allow_html=True)
        st.subheader("🎯 Data Quality Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Confidence thresholds
            st.write("**Confidence Thresholds:**")
            min_confidence = st.slider(
                "Minimum Confidence Score",
                min_value=0.0,
                max_value=1.0,
                value=existing_config.get('min_confidence', 0.7),
                step=0.05,
                help="Minimum confidence score to include a result"
            )
            
            # Duplicate handling
            handle_duplicates = st.checkbox(
                "Remove Duplicate Records",
                value=existing_config.get('handle_duplicates', True),
                help="Automatically remove duplicate records based on name and address"
            )
            
            # Data validation
            validate_data = st.checkbox(
                "Validate Extracted Data",
                value=existing_config.get('validate_data', True),
                help="Validate phone numbers, emails, and addresses"
            )
        
        with col2:
            # Output format
            st.write("**Output Settings:**")
            output_format = st.selectbox(
                "Output Format",
                ["CSV", "Excel", "Both"],
                index=0 if existing_config.get('output_format') is None else 
                      ["CSV", "Excel", "Both"].index(existing_config.get('output_format', 'CSV'))
            )
            
            # Include original data
            include_original = st.checkbox(
                "Include Original Data",
                value=existing_config.get('include_original', True),
                help="Include original farm report data in output"
            )
            
            # Create summary report
            create_summary = st.checkbox(
                "Create Summary Report",
                value=existing_config.get('create_summary', True),
                help="Generate a summary report of enrichment results"
            )
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Advanced settings
        with st.expander("🔧 Advanced Settings", expanded=False):
            st.subheader("Advanced Configuration")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Proxy settings
                use_proxy = st.checkbox(
                    "Use Proxy",
                    value=existing_config.get('use_proxy', False),
                    help="Use proxy for requests (configure in .env file)"
                )
                
                # User agent rotation
                rotate_user_agents = st.checkbox(
                    "Rotate User Agents",
                    value=existing_config.get('rotate_user_agents', True),
                    help="Rotate user agents to avoid detection"
                )
                
                # Session management
                maintain_sessions = st.checkbox(
                    "Maintain Sessions",
                    value=existing_config.get('maintain_sessions', True),
                    help="Maintain browser sessions between requests"
                )
            
            with col2:
                # Error handling
                continue_on_error = st.checkbox(
                    "Continue on Error",
                    value=existing_config.get('continue_on_error', True),
                    help="Continue processing even if some requests fail"
                )
                
                # Logging level
                logging_level = st.selectbox(
                    "Logging Level",
                    ["INFO", "DEBUG", "WARNING", "ERROR"],
                    index=0 if existing_config.get('logging_level') is None else 
                          ["INFO", "DEBUG", "WARNING", "ERROR"].index(existing_config.get('logging_level', 'INFO'))
                )
                
                # Save intermediate results
                save_intermediate = st.checkbox(
                    "Save Intermediate Results",
                    value=existing_config.get('save_intermediate', True),
                    help="Save results every 10 processed records"
                )
    
    with col2:
        st.markdown('<div class="config-card">', unsafe_allow_html=True)
        st.subheader("📋 Configuration Summary")
        
        # Display current configuration
        config_summary = {
            "Lead Type": lead_type,
            "Max Leads": max_leads,
            "Batch Size": batch_size,
            "Min Delay": f"{min_delay}s",
            "Max Delay": f"{max_delay}s",
            "Max Retries": max_retries,
            "Min Confidence": f"{min_confidence:.2f}",
            "Output Format": output_format,
            "Active Sources": len([s for s in selected_sources.values() if s['enabled']])
        }
        
        for key, value in config_summary.items():
            st.write(f"**{key}:** {value}")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Action buttons
        st.markdown('<div class="config-card">', unsafe_allow_html=True)
        st.subheader("🚀 Actions")
        
        # Save configuration
        if st.button("💾 Save Configuration", type="primary"):
            config = {
                'lead_type': lead_type,
                'max_leads': max_leads,
                'batch_size': batch_size,
                'min_delay': min_delay,
                'max_delay': max_delay,
                'max_retries': max_retries,
                'min_confidence': min_confidence,
                'handle_duplicates': handle_duplicates,
                'validate_data': validate_data,
                'output_format': output_format,
                'include_original': include_original,
                'create_summary': create_summary,
                'use_proxy': use_proxy,
                'rotate_user_agents': rotate_user_agents,
                'maintain_sessions': maintain_sessions,
                'continue_on_error': continue_on_error,
                'logging_level': logging_level,
                'save_intermediate': save_intermediate,
                'sources': selected_sources
            }
            
            if save_config(config):
                st.success("✅ Configuration saved successfully!")
            else:
                st.error("❌ Failed to save configuration")
        
        # Load configuration
        if st.button("📂 Load Configuration"):
            loaded_config = load_config()
            if loaded_config:
                st.success("✅ Configuration loaded successfully!")
                st.json(loaded_config)
            else:
                st.warning("⚠️ No saved configuration found")
        
        # Reset configuration
        if st.button("🔄 Reset to Defaults"):
            st.info("Configuration reset to default values")
            st.rerun()
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Next steps
        st.markdown('<div class="config-card">', unsafe_allow_html=True)
        st.subheader("📈 Next Steps")
        st.write("1. **Save your configuration**")
        st.write("2. **Upload your farm report** in Phase 2")
        st.write("3. **Start enrichment process**")
        st.write("4. **Review results** and adjust settings")
        
        if st.button("🚀 Start Enrichment Process", type="secondary"):
            st.info("Navigate to the enrichment page to start processing")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Configuration preview
    if st.checkbox("👁️ Show Configuration Preview", value=False):
        st.markdown('<div class="parameter-section">', unsafe_allow_html=True)
        st.subheader("📄 Configuration Preview")
        
        preview_config = {
            "lead_type": lead_type,
            "max_leads": max_leads,
            "batch_size": batch_size,
            "timing": {
                "min_delay": min_delay,
                "max_delay": max_delay,
                "max_retries": max_retries
            },
            "quality": {
                "min_confidence": min_confidence,
                "handle_duplicates": handle_duplicates,
                "validate_data": validate_data
            },
            "output": {
                "format": output_format,
                "include_original": include_original,
                "create_summary": create_summary
            },
            "sources": selected_sources
        }
        
        st.json(preview_config)
        st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main() 