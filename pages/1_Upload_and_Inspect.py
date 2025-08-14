import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import io

# Page configuration
st.set_page_config(
    page_title="Upload & Inspect - Farm Reports",
    page_icon="📁",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .upload-section {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
    }
    .stats-card {
        background-color: #ffffff;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .data-preview {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

def analyze_farm_report(df):
    """Analyze Farm Report data and return comprehensive statistics"""
    
    stats = {}
    
    # Basic file statistics
    stats['total_rows'] = len(df)
    stats['total_columns'] = len(df.columns)
    stats['file_size_mb'] = df.memory_usage(deep=True).sum() / 1024 / 1024
    
    # Column analysis
    stats['columns'] = list(df.columns)
    # Convert data types to strings to avoid serialization issues
    stats['data_types'] = {col: str(dtype) for col, dtype in df.dtypes.items()}
    
    # Missing data analysis
    missing_data = df.isnull().sum()
    stats['missing_data'] = missing_data.to_dict()
    stats['total_missing'] = missing_data.sum()
    
    # Unique values analysis
    stats['unique_counts'] = {}
    for col in df.columns:
        if df[col].dtype in ['object', 'string']:
            stats['unique_counts'][col] = df[col].nunique()
    
    # Try to identify key columns for Farm Reports
    stats['potential_key_columns'] = []
    stats['borrower_columns'] = []
    stats['loan_amount_columns'] = []
    
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['name', 'borrower', 'lender', 'owner', 'property', 'address']):
            stats['potential_key_columns'].append(col)
        
        # Identify borrower columns
        if any(keyword in col_lower for keyword in ['borrower', 'name', 'owner']):
            stats['borrower_columns'].append(col)
        
        # Identify loan amount columns
        if any(keyword in col_lower for keyword in ['loan', 'amount', 'value', 'principal']):
            stats['loan_amount_columns'].append(col)
    
    return stats

def create_summary_visualizations(df, stats):
    """Create visualizations for the Farm Report data"""
    
    try:
        # Create tabs for different visualizations
        tab1, tab2, tab3 = st.tabs(["📊 Data Overview", "📈 Missing Data", "🔍 Column Analysis"])
        
        with tab1:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Rows", f"{stats['total_rows']:,}")
            
            with col2:
                st.metric("Total Columns", stats['total_columns'])
            
            with col3:
                st.metric("File Size", f"{stats['file_size_mb']:.2f} MB")
            
            # Data types distribution
            try:
                dtype_counts = pd.Series(stats['data_types']).value_counts()
                if len(dtype_counts) > 0:
                    fig_dtypes = px.pie(
                        values=dtype_counts.values,
                        names=dtype_counts.index,
                        title="Data Types Distribution"
                    )
                    st.plotly_chart(fig_dtypes, use_container_width=True)
                else:
                    st.info("No data type information available")
            except Exception as e:
                st.warning(f"Could not create data type visualization: {str(e)}")
                st.info("Data type information is available in the table below")
        
        with tab2:
            # Missing data visualization
            try:
                missing_df = pd.DataFrame({
                    'Column': list(stats['missing_data'].keys()),
                    'Missing_Count': list(stats['missing_data'].values())
                })
                missing_df['Missing_Percentage'] = (missing_df['Missing_Count'] / stats['total_rows']) * 100
                
                fig_missing = px.bar(
                    missing_df,
                    x='Column',
                    y='Missing_Percentage',
                    title="Missing Data by Column (%)",
                    color='Missing_Percentage',
                    color_continuous_scale='Reds'
                )
                fig_missing.update_xaxes(tickangle=45)
                st.plotly_chart(fig_missing, use_container_width=True)
            except Exception as e:
                st.warning(f"Could not create missing data visualization: {str(e)}")
        
        with tab3:
            # Column analysis
            try:
                if stats['unique_counts']:
                    unique_df = pd.DataFrame({
                        'Column': list(stats['unique_counts'].keys()),
                        'Unique_Values': list(stats['unique_counts'].values())
                    })
                    
                    fig_unique = px.bar(
                        unique_df,
                        x='Column',
                        y='Unique_Values',
                        title="Unique Values by Column"
                    )
                    fig_unique.update_xaxes(tickangle=45)
                    st.plotly_chart(fig_unique, use_container_width=True)
                else:
                    st.info("No unique value analysis available")
            except Exception as e:
                st.warning(f"Could not create column analysis visualization: {str(e)}")
        
    except Exception as e:
        st.error(f"Error creating visualizations: {str(e)}")
        st.info("Please check your data format and try again")

def create_borrower_analysis(df, stats):
    """Create borrower analysis visualizations"""
    
    try:
        st.subheader("🏦 Borrower Analysis")
        
        # Find the best borrower and loan amount columns
        borrower_col = None
        loan_amount_col = None
        
        # Try to find borrower column
        for col in stats.get('borrower_columns', []):
            if col in df.columns:
                borrower_col = col
                break
        
        # Try to find loan amount column
        for col in stats.get('loan_amount_columns', []):
            if col in df.columns:
                loan_amount_col = col
                break
        
        if borrower_col is None:
            # Try to find any column that might contain borrower names
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['borrower', 'name', 'owner', 'client']):
                    borrower_col = col
                    break
        
        if loan_amount_col is None:
            # Try to find any column that might contain loan amounts
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['loan', 'amount', 'value', 'principal', 'balance']):
                    loan_amount_col = col
                    break
        
        if borrower_col and loan_amount_col:
            # Clean and prepare data
            df_clean = df.copy()
            
            # Convert loan amount to numeric, removing any currency symbols
            df_clean[loan_amount_col] = pd.to_numeric(
                df_clean[loan_amount_col].astype(str).str.replace('$', '').str.replace(',', '').str.replace('(', '-').str.replace(')', ''),
                errors='coerce'
            )
            
            # Remove rows with missing borrower names or loan amounts
            df_clean = df_clean.dropna(subset=[borrower_col, loan_amount_col])
            
            if len(df_clean) > 0:
                # Top 20 Borrowers by Loan Amount
                top_by_amount = df_clean.groupby(borrower_col)[loan_amount_col].sum().sort_values(ascending=False).head(20)
                
                # Top 20 Borrowers by Loan Count
                top_by_count = df_clean.groupby(borrower_col).size().sort_values(ascending=False).head(20)
                
                # Create visualizations
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("💰 Top 20 Borrowers by Loan Amount")
                    
                    if len(top_by_amount) > 0:
                        fig_amount = px.bar(
                            x=top_by_amount.values,
                            y=top_by_amount.index,
                            orientation='h',
                            title=f"Top 20 Borrowers by Total Loan Amount",
                            labels={'x': 'Total Loan Amount ($)', 'y': 'Borrower Name'},
                            color=top_by_amount.values,
                            color_continuous_scale='Greens'
                        )
                        fig_amount.update_layout(height=600)
                        st.plotly_chart(fig_amount, use_container_width=True)
                        
                        # Show table
                        st.write("**Top 10 by Amount:**")
                        amount_df = pd.DataFrame({
                            'Borrower': top_by_amount.index[:10],
                            'Total Loan Amount': [f"${val:,.2f}" for val in top_by_amount.values[:10]]
                        })
                        st.dataframe(amount_df, use_container_width=True)
                    else:
                        st.info("No loan amount data available for analysis")
                
                with col2:
                    st.subheader("📊 Top 20 Borrowers by Loan Count")
                    
                    if len(top_by_count) > 0:
                        fig_count = px.bar(
                            x=top_by_count.values,
                            y=top_by_count.index,
                            orientation='h',
                            title=f"Top 20 Borrowers by Number of Loans",
                            labels={'x': 'Number of Loans', 'y': 'Borrower Name'},
                            color=top_by_count.values,
                            color_continuous_scale='Blues'
                        )
                        fig_count.update_layout(height=600)
                        st.plotly_chart(fig_count, use_container_width=True)
                        
                        # Show table
                        st.write("**Top 10 by Count:**")
                        count_df = pd.DataFrame({
                            'Borrower': top_by_count.index[:10],
                            'Number of Loans': top_by_count.values[:10]
                        })
                        st.dataframe(count_df, use_container_width=True)
                    else:
                        st.info("No loan count data available for analysis")
                
                # Summary statistics
                st.markdown("---")
                st.subheader("📈 Summary Statistics")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Unique Borrowers", f"{df_clean[borrower_col].nunique():,}")
                
                with col2:
                    st.metric("Total Loan Amount", f"${df_clean[loan_amount_col].sum():,.2f}")
                
                with col3:
                    st.metric("Average Loan Amount", f"${df_clean[loan_amount_col].mean():,.2f}")
                
                with col4:
                    st.metric("Total Number of Loans", f"{len(df_clean):,}")
                
            else:
                st.warning("⚠️ No valid loan data found for analysis")
                st.write("Please ensure your data contains borrower names and loan amounts")
        
        else:
            st.warning("⚠️ Could not identify borrower or loan amount columns")
            st.write("**Identified columns:**")
            st.write(f"• Borrower columns: {stats.get('borrower_columns', [])}")
            st.write(f"• Loan amount columns: {stats.get('loan_amount_columns', [])}")
            st.write("Please check your column names and ensure they contain borrower names and loan amounts")
    
    except Exception as e:
        st.error(f"Error creating borrower analysis: {str(e)}")
        st.info("Please check your data format and column names")

def main():
    st.title("📁 Upload & Inspect Farm Reports")
    st.markdown("**Phase 2:** Upload + Preview UI")
    
    # File upload section
    st.markdown('<div class="upload-section">', unsafe_allow_html=True)
    st.subheader("📤 Upload Your Farm Report")
    
    uploaded_file = st.file_uploader(
        "Choose a Farm Report file",
        type=['csv', 'xlsx', 'xls'],
        help="Upload your Farm Report file (CSV, Excel)"
    )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    if uploaded_file is not None:
        try:
            # Load the data
            with st.spinner("Loading and analyzing your Farm Report..."):
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                # Clean the dataframe to avoid serialization issues
                df = df.copy()
                
                # Convert problematic data types
                for col in df.columns:
                    if df[col].dtype == 'object':
                        # Convert to string to avoid serialization issues
                        df[col] = df[col].astype(str)
                
                # Handle any remaining problematic data types
                for col in df.columns:
                    try:
                        # Try to convert to a safe type
                        if df[col].dtype == 'object':
                            df[col] = df[col].fillna('').astype(str)
                    except Exception:
                        # If conversion fails, force to string
                        df[col] = df[col].astype(str)
            
            st.success(f"✅ File uploaded successfully!")
            
            # Analyze the data
            stats = analyze_farm_report(df)
            
            # Display file information
            st.markdown('<div class="stats-card">', unsafe_allow_html=True)
            st.subheader("📊 File Information")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("File Name", uploaded_file.name)
            
            with col2:
                st.metric("Total Rows", f"{stats['total_rows']:,}")
            
            with col3:
                st.metric("Total Columns", stats['total_columns'])
            
            with col4:
                st.metric("File Size", f"{stats['file_size_mb']:.2f} MB")
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Data preview section
            st.markdown('<div class="data-preview">', unsafe_allow_html=True)
            st.subheader("🔍 Data Preview")
            
            # Show first few rows
            st.write("**First 5 rows of your data:**")
            try:
                # Create a safe copy for display
                df_display = df.head().copy()
                for col in df_display.columns:
                    df_display[col] = df_display[col].astype(str)
                st.dataframe(df_display, use_container_width=True)
            except Exception as e:
                st.warning(f"Could not display data preview: {str(e)}")
                st.write("Data loaded successfully but preview display failed")
            
            # Show column information
            st.write("**Column Information:**")
            col_info = pd.DataFrame({
                'Column': df.columns,
                'Data Type': [str(dtype) for dtype in df.dtypes],
                'Non-Null Count': df.count().values,
                'Missing Count': df.isnull().sum().values,
                'Missing %': (df.isnull().sum() / len(df) * 100).round(2).values
            })
            st.dataframe(col_info, use_container_width=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Key columns identification
            if stats['potential_key_columns']:
                st.markdown('<div class="stats-card">', unsafe_allow_html=True)
                st.subheader("🎯 Identified Key Columns")
                st.write("The following columns appear to contain important information:")
                
                for col in stats['potential_key_columns']:
                    st.write(f"• **{col}** - {df[col].nunique()} unique values")
                    if df[col].dtype == 'object':
                        sample_values = df[col].dropna().head(3).tolist()
                        st.write(f"  Sample values: {', '.join(map(str, sample_values))}")
                
                # Show borrower and loan amount columns specifically
                if stats.get('borrower_columns'):
                    st.write("**🏦 Borrower Columns:**")
                    for col in stats['borrower_columns']:
                        st.write(f"• **{col}** - {df[col].nunique()} unique borrowers")
                
                if stats.get('loan_amount_columns'):
                    st.write("**💰 Loan Amount Columns:**")
                    for col in stats['loan_amount_columns']:
                        st.write(f"• **{col}** - {df[col].nunique()} unique values")
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Missing data summary
            if stats['total_missing'] > 0:
                st.markdown('<div class="stats-card">', unsafe_allow_html=True)
                st.subheader("⚠️ Missing Data Summary")
                st.write(f"Total missing values: {stats['total_missing']:,}")
                
                # Show columns with missing data
                missing_cols = {k: v for k, v in stats['missing_data'].items() if v > 0}
                if missing_cols:
                    st.write("**Columns with missing data:**")
                    for col, count in missing_cols.items():
                        percentage = (count / stats['total_rows']) * 100
                        st.write(f"• **{col}**: {count:,} missing ({percentage:.1f}%)")
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Create visualizations
            st.subheader("📈 Data Visualizations")
            create_summary_visualizations(df, stats)
            
            # Create borrower analysis
            create_borrower_analysis(df, stats)
            
            # Data quality assessment
            st.markdown('<div class="stats-card">', unsafe_allow_html=True)
            st.subheader("🔍 Data Quality Assessment")
            
            # Calculate data quality score
            total_cells = stats['total_rows'] * stats['total_columns']
            completeness_score = ((total_cells - stats['total_missing']) / total_cells) * 100
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Data Completeness", f"{completeness_score:.1f}%")
            
            with col2:
                st.metric("Columns with Missing Data", len([v for v in stats['missing_data'].values() if v > 0]))
            
            with col3:
                st.metric("Potential Key Columns", len(stats['potential_key_columns']))
            
            # Data quality recommendations
            st.write("**Recommendations:**")
            if completeness_score < 90:
                st.warning("⚠️ Data has significant missing values. Consider data cleaning before processing.")
            else:
                st.success("✅ Data completeness looks good!")
            
            if len(stats['potential_key_columns']) == 0:
                st.warning("⚠️ No key columns identified. Please verify column names.")
            else:
                st.success(f"✅ Identified {len(stats['potential_key_columns'])} potential key columns.")
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Next steps
            st.markdown('<div class="stats-card">', unsafe_allow_html=True)
            st.subheader("🚀 Next Steps")
            st.write("Your Farm Report has been successfully uploaded and analyzed. You can now:")
            st.write("1. **Review the data preview** above")
            st.write("2. **Check the visualizations** for insights")
            st.write("3. **Proceed to Phase 3** for parameter configuration")
            st.write("4. **Start enrichment process** when ready")
            
            if st.button("⚙️ Configure Parameters (Phase 3)", type="primary"):
                st.info("Navigate to the Parameter Form page to configure enrichment settings.")
            
            st.markdown('</div>', unsafe_allow_html=True)
            
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")
            st.write("Please ensure your file is in a supported format (CSV, Excel) and try again.")
    
    else:
        st.info("📤 Please upload a Farm Report file to begin analysis")
        
        # Show example of expected format
        st.markdown('<div class="stats-card">', unsafe_allow_html=True)
        st.subheader("📋 Expected File Format")
        st.write("Your Farm Report should contain columns such as:")
        st.write("• Borrower/Lender names")
        st.write("• Property addresses")
        st.write("• Contact information")
        st.write("• Financial data")
        st.write("• Property details")
        st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main() 