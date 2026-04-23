import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
from typing import Optional, Dict, Any

from modules import AdvancedDataProcessor, ChartGenerator
from modules.data_models import DataType, DatasetInfo


st.set_page_config(
    page_title="销售数据分析看板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 1rem;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    .metric-card-good {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
    }
    .metric-card-warning {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
    }
    .metric-card-danger {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #6c757d;
    }
    .section-header {
        font-size: 1.5rem;
        color: #1f77b4;
        margin-top: 2rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e9ecef;
    }
    .upload-section {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 1.5rem;
        margin-bottom: 1rem;
    }
    .anomaly-card {
        border-radius: 8px;
        padding: 0.75rem;
        margin-bottom: 0.5rem;
    }
    .anomaly-high {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
    }
    .anomaly-medium {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
    }
    </style>
""", unsafe_allow_html=True)


def format_currency(value: float) -> str:
    if value is None or pd.isna(value):
        return "¥0.00"
    if value >= 100000000:
        return f"¥{value/100000000:.2f}亿"
    elif value >= 10000:
        return f"¥{value/10000:.2f}万"
    else:
        return f"¥{value:,.2f}"


def format_percent(value: float, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "0.0%"
    return f"{value * 100:.{decimals}f}%"


def to_csv(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    df.to_csv(output, index=False, encoding='utf-8-sig')
    return output.getvalue()


def get_achievement_status(rate: float) -> str:
    if rate >= 1.0:
        return "good"
    elif rate >= 0.8:
        return "warning"
    else:
        return "danger"


def init_session_state():
    if 'processor' not in st.session_state:
        st.session_state.processor = AdvancedDataProcessor()
    if 'chart_generator' not in st.session_state:
        st.session_state.chart_generator = ChartGenerator()
    if 'data_ready' not in st.session_state:
        st.session_state.data_ready = False
    if 'dataset_count' not in st.session_state:
        st.session_state.dataset_count = 0


def display_upload_section():
    st.markdown('<div class="section-header">📁 数据导入</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="upload-section">
            <h4>📋 订单数据 (必需)</h4>
            <p>包含订单日期、销售额、产品名等核心交易数据</p>
        </div>
        """, unsafe_allow_html=True)
        orders_files = st.file_uploader(
            "上传订单数据", 
            type=['csv'], 
            key="orders_upload",
            accept_multiple_files=True
        )
    
    with col2:
        st.markdown("""
        <div class="upload-section">
            <h4>📦 商品数据 (可选)</h4>
            <p>包含产品成本、类别、品牌等信息，用于关联计算利润</p>
        </div>
        """, unsafe_allow_html=True)
        products_file = st.file_uploader(
            "上传商品数据", 
            type=['csv'], 
            key="products_upload"
        )
    
    with col3:
        st.markdown("""
        <div class="upload-section">
            <h4>🎯 目标数据 (可选)</h4>
            <p>包含销售目标，用于目标达成分析</p>
        </div>
        """, unsafe_allow_html=True)
        targets_file = st.file_uploader(
            "上传目标数据", 
            type=['csv'], 
            key="targets_upload"
        )
    
    if orders_files or products_file or targets_file:
        process_button = st.button("🚀 开始数据处理", use_container_width=True, type="primary")
        
        if process_button:
            with st.spinner("正在处理数据..."):
                processor = st.session_state.processor
                processor = AdvancedDataProcessor()
                st.session_state.processor = processor
                
                if orders_files:
                    for file in orders_files:
                        result = processor.load_dataset(file, file.name)
                        if result.success:
                            st.success(result.message)
                        else:
                            st.error(result.message)
                
                if products_file:
                    result = processor.load_dataset(products_file, products_file.name)
                    if result.success:
                        st.success(result.message)
                    else:
                        st.error(result.message)
                
                if targets_file:
                    result = processor.load_dataset(targets_file, targets_file.name)
                    if result.success:
                        st.success(result.message)
                    else:
                        st.error(result.message)
                
                result = processor.process_and_unify()
                
                if result.success:
                    st.session_state.data_ready = True
                    st.success(result.message)
                    
                    if result.warnings:
                        for warning in result.warnings:
                            st.warning(warning)
                    
                    stats = processor.get_cleaning_stats()
                    with st.expander("📊 数据处理统计"):
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("原始数据", f"{stats['original_count']:,}")
                        col2.metric("删除重复", f"{stats['duplicates_removed']:,}")
                        col3.metric("删除无效", f"{stats['null_rows_removed']:,}")
                        col4.metric("有效数据", f"{stats['final_count']:,}")
                    
                    st.rerun()
                else:
                    st.error(result.message)
    
    with st.expander("📋 字段说明与示例数据"):
        st.markdown("""
        ### 支持的字段名（不区分大小写和英文写法）
        
        **订单数据必需字段：**
        - 订单日期 / 下单时间 / date / order_date
        - 销售额 / 销售金额 / 金额 / sales / amount
        
        **订单数据可选字段：**
        - 产品名 / 商品名 / product / product_name
        - 地区 / 区域 / region / area
        - 类别 / 分类 / category
        - 利润 / profit
        - 数量 / quantity
        - 销售人员 / 销售员 / salesperson
        
        **商品数据字段：**
        - 产品名 / product_name
        - 类别 / category
        - 成本 / cost
        - 品牌 / brand
        
        **目标数据字段：**
        - 年月 / 月份 / month
        - 目标销售额 / 销售目标 / target / sales_target
        - 地区 / 类别 / 销售人员（用于细分目标）
        """)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### 📥 下载订单示例")
            orders_sample = generate_sample_orders()
            st.download_button(
                label="订单示例数据",
                data=to_csv(orders_sample),
                file_name="sample_orders.csv",
                mime="text/csv"
            )
        
        with col2:
            st.markdown("#### 📥 下载商品示例")
            products_sample = generate_sample_products()
            st.download_button(
                label="商品示例数据",
                data=to_csv(products_sample),
                file_name="sample_products.csv",
                mime="text/csv"
            )
        
        with col3:
            st.markdown("#### 📥 下载目标示例")
            targets_sample = generate_sample_targets()
            st.download_button(
                label="目标示例数据",
                data=to_csv(targets_sample),
                file_name="sample_targets.csv",
                mime="text/csv"
            )


def generate_sample_orders(n_rows: int = 500) -> pd.DataFrame:
    np.random.seed(42)
    
    regions = ['华东', '华北', '华南', '西南', '西北', '东北']
    categories = ['电子产品', '服装', '家居用品', '食品饮料', '美妆护肤']
    salespeople = ['张三', '李四', '王五', '赵六', '钱七', '孙八', '周九', '吴十']
    
    products = {
        '电子产品': ['iPhone 15', 'MacBook Pro', 'iPad Air', 'AirPods Pro', '小米手机', '华为Mate', '三星Galaxy', '联想笔记本'],
        '服装': ['Nike运动鞋', 'Adidas卫衣', 'Levis牛仔裤', 'Uniqlo外套', '安踏跑步鞋', '李宁T恤', '波司登羽绒服', '海澜之家衬衫'],
        '家居用品': ['宜家沙发', '慕思床垫', '全友衣柜', '美的空调', '海尔冰箱', '小米电视', '戴森吸尘器', '飞利浦电动牙刷'],
        '食品饮料': ['蒙牛牛奶', '伊利酸奶', '农夫山泉', '可口可乐', '康师傅方便面', '统一奶茶', '三只松鼠坚果', '良品铺子零食'],
        '美妆护肤': ['兰蔻小黑瓶', '雅诗兰黛眼霜', 'SK-II神仙水', '资生堂防晒', '欧莱雅精华', '玉兰油面霜', '完美日记口红', '花西子粉饼']
    }
    
    data = []
    start_date = pd.Timestamp('2024-01-01')
    end_date = pd.Timestamp('2024-12-31')
    
    for _ in range(n_rows):
        days = (end_date - start_date).days
        random_days = np.random.randint(0, days)
        order_date = start_date + pd.Timedelta(days=random_days)
        
        category = np.random.choice(categories)
        product = np.random.choice(products[category])
        
        if category == '电子产品':
            sales_amount = np.random.uniform(2000, 15000)
        elif category == '家居用品':
            sales_amount = np.random.uniform(1000, 10000)
        elif category == '服装':
            sales_amount = np.random.uniform(200, 3000)
        elif category == '美妆护肤':
            sales_amount = np.random.uniform(100, 2000)
        else:
            sales_amount = np.random.uniform(50, 500)
        
        profit = sales_amount * np.random.uniform(0.15, 0.35)
        quantity = np.random.randint(1, 5)
        
        data.append({
            '订单日期': order_date.strftime('%Y-%m-%d'),
            '地区': np.random.choice(regions),
            '类别': category,
            '产品名': product,
            '销售额': round(sales_amount, 2),
            '利润': round(profit, 2),
            '数量': quantity,
            '销售人员': np.random.choice(salespeople)
        })
    
    return pd.DataFrame(data)


def generate_sample_products() -> pd.DataFrame:
    products_data = [
        {'产品名': 'iPhone 15', '类别': '电子产品', '成本': 6500, '售价': 8999, '品牌': 'Apple'},
        {'产品名': 'MacBook Pro', '类别': '电子产品', '成本': 11000, '售价': 14999, '品牌': 'Apple'},
        {'产品名': 'iPad Air', '类别': '电子产品', '成本': 3500, '售价': 4799, '品牌': 'Apple'},
        {'产品名': 'AirPods Pro', '类别': '电子产品', '成本': 1300, '售价': 1799, '品牌': 'Apple'},
        {'产品名': '小米手机', '类别': '电子产品', '成本': 2800, '售价': 3999, '品牌': '小米'},
        {'产品名': '华为Mate', '类别': '电子产品', '成本': 4200, '售价': 5999, '品牌': '华为'},
        {'产品名': 'Nike运动鞋', '类别': '服装', '成本': 600, '售价': 899, '品牌': 'Nike'},
        {'产品名': 'Adidas卫衣', '类别': '服装', '成本': 400, '售价': 599, '品牌': 'Adidas'},
        {'产品名': 'Levis牛仔裤', '类别': '服装', '成本': 280, '售价': 399, '品牌': 'Levis'},
        {'产品名': '美的空调', '类别': '家居用品', '成本': 3200, '售价': 4599, '品牌': '美的'},
        {'产品名': '海尔冰箱', '类别': '家居用品', '成本': 2800, '售价': 3999, '品牌': '海尔'},
        {'产品名': '兰蔻小黑瓶', '类别': '美妆护肤', '成本': 750, '售价': 1080, '品牌': '兰蔻'},
        {'产品名': '雅诗兰黛眼霜', '类别': '美妆护肤', '成本': 550, '售价': 780, '品牌': '雅诗兰黛'},
    ]
    return pd.DataFrame(products_data)


def generate_sample_targets() -> pd.DataFrame:
    months = ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06',
              '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12']
    regions = ['华东', '华北', '华南', '西南', '西北', '东北']
    
    targets = []
    for month in months:
        month_idx = int(month.split('-')[1])
        seasonal_factor = 1.0 + 0.3 * (1 if month_idx in [1, 2, 11, 12] else 0)
        
        targets.append({
            '年月': month,
            '地区': '',
            '类别': '',
            '目标销售额': 500000 * seasonal_factor,
            '目标利润': 125000 * seasonal_factor
        })
        
        for region in regions:
            region_factor = {'华东': 1.5, '华北': 1.2, '华南': 1.3, '西南': 0.8, '西北': 0.6, '东北': 0.7}[region]
            targets.append({
                '年月': month,
                '地区': region,
                '类别': '',
                '目标销售额': 100000 * seasonal_factor * region_factor,
                '目标利润': 25000 * seasonal_factor * region_factor
            })
    
    return pd.DataFrame(targets)


def display_overview_tab(filtered_df: pd.DataFrame, metrics: Dict[str, Any], target_info: Dict[str, Any]):
    st.markdown('<div class="section-header">📈 整体概览</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{format_currency(metrics['total_sales'])}</div>
            <div class="metric-label">💰 总销售额</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{format_currency(metrics['total_profit'])}</div>
            <div class="metric-label">📊 总利润</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{metrics['order_count']:,}</div>
            <div class="metric-label">📋 订单数</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{format_percent(metrics['profit_rate'])}</div>
            <div class="metric-label">📌 利润率</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col5:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{format_currency(metrics['avg_order_sales'])}</div>
            <div class="metric-label">💵 客单价</div>
        </div>
        """, unsafe_allow_html=True)
    
    if target_info.get('has_target', False):
        st.markdown('<div class="section-header">🎯 目标达成</div>', unsafe_allow_html=True)
        
        status = get_achievement_status(target_info['achievement_rate'])
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            status_class = f"metric-card metric-card-{status}"
            st.markdown(f"""
            <div class="{status_class}">
                <div class="metric-value" style="font-size: 2.5rem;">{format_percent(target_info['achievement_rate'])}</div>
                <div class="metric-label">销售额达成率</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.metric("目标销售额", format_currency(target_info['target_sales']))
            st.metric("实际销售额", format_currency(target_info['actual_sales']))
        
        with col3:
            chart = st.session_state.chart_generator.target_achievement_chart(
                actual_sales=target_info['actual_sales'],
                target_sales=target_info['target_sales'],
                actual_profit=metrics['total_profit'],
                target_profit=target_info.get('target_profit', 0)
            )
            if chart:
                st.plotly_chart(chart, use_container_width=True)
    
    st.markdown('<div class="section-header">📊 关键图表</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        trend_chart = st.session_state.chart_generator.sales_trend_chart(filtered_df)
        if trend_chart:
            st.plotly_chart(trend_chart, use_container_width=True)
    
    with col2:
        if target_info.get('has_target', False):
            processor = st.session_state.processor
            trend_data = processor.analyze_trends(filtered_df)
            target_data = processor.get_target_data()
            vs_chart = st.session_state.chart_generator.actual_vs_target_chart(trend_data, target_data)
            if vs_chart:
                st.plotly_chart(vs_chart, use_container_width=True)
        else:
            if '地区' in filtered_df.columns:
                region_chart = st.session_state.chart_generator.region_comparison_chart(filtered_df)
                if region_chart:
                    st.plotly_chart(region_chart, use_container_width=True)


def display_dimension_tab(filtered_df: pd.DataFrame):
    st.markdown('<div class="section-header">🔍 多维度分析</div>', unsafe_allow_html=True)
    
    dimensions = []
    for col in ['地区', '类别', '销售人员', '品牌']:
        if col in filtered_df.columns:
            unique_vals = filtered_df[col].dropna().unique()
            if len(unique_vals) > 1:
                dimensions.append(col)
    
    if not dimensions:
        st.info("当前数据没有足够的维度进行分析")
        return
    
    selected_dim = st.radio("选择分析维度", dimensions, horizontal=True)
    
    processor = st.session_state.processor
    dim_data = processor.analyze_by_dimension(filtered_df, selected_dim)
    
    if dim_data.empty:
        st.warning("该维度下没有数据")
        return
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        chart = st.session_state.chart_generator.dimension_comparison_chart(
            dim_data, selected_dim, f"{selected_dim}销售分析"
        )
        if chart:
            st.plotly_chart(chart, use_container_width=True)
    
    with col2:
        st.markdown(f"#### 📊 {selected_dim}详细数据")
        
        display_df = dim_data.copy()
        for col in ['销售额', '利润']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(format_currency)
        for col in ['销售额占比', '利润占比', '利润率']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )


def display_products_tab(filtered_df: pd.DataFrame):
    st.markdown('<div class="section-header">📦 产品分析</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        top_n = st.slider("显示Top N产品", min_value=5, max_value=20, value=10)
        product_chart = st.session_state.chart_generator.product_performance_chart(filtered_df, top_n)
        if product_chart:
            st.plotly_chart(product_chart, use_container_width=True)
    
    with col2:
        if '类别' in filtered_df.columns:
            category_chart = st.session_state.chart_generator.category_analysis_chart(filtered_df)
            if category_chart:
                st.plotly_chart(category_chart, use_container_width=True)
    
    st.markdown('<div class="section-header">📋 产品明细</div>', unsafe_allow_html=True)
    
    processor = st.session_state.processor
    product_data = processor.analyze_by_dimension(filtered_df, '产品名')
    
    if not product_data.empty:
        display_df = product_data.copy()
        display_df['销售额'] = display_df['销售额'].apply(format_currency)
        display_df['利润'] = display_df['利润'].apply(format_currency)
        display_df['利润率'] = display_df['利润率'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )


def display_anomalies_tab(filtered_df: pd.DataFrame):
    st.markdown('<div class="section-header">⚠️ 异常波动检测</div>', unsafe_allow_html=True)
    
    processor = st.session_state.processor
    anomalies = processor.detect_anomalies(filtered_df)
    
    if not anomalies:
        st.success("🎉 未检测到明显的异常波动")
        return
    
    high_anomalies = [a for a in anomalies if a.get('severity') == 'high']
    medium_anomalies = [a for a in anomalies if a.get('severity') == 'medium']
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("🔴 高风险异常", len(high_anomalies))
    with col2:
        st.metric("🟡 中风险异常", len(medium_anomalies))
    
    if high_anomalies:
        st.markdown("##### 🔴 高风险异常")
        for anomaly in high_anomalies:
            st.markdown(f"""
            <div class="anomaly-card anomaly-high">
                <strong>{anomaly.get('message', '')}</strong>
                <br><small>类型: {anomaly.get('type', 'unknown')}</small>
            </div>
            """, unsafe_allow_html=True)
    
    if medium_anomalies:
        st.markdown("##### 🟡 中风险异常")
        for anomaly in medium_anomalies:
            st.markdown(f"""
            <div class="anomaly-card anomaly-medium">
                <strong>{anomaly.get('message', '')}</strong>
                <br><small>类型: {anomaly.get('type', 'unknown')}</small>
            </div>
            """, unsafe_allow_html=True)
    
    anomaly_chart = st.session_state.chart_generator.anomaly_detection_chart(anomalies)
    if anomaly_chart:
        st.plotly_chart(anomaly_chart, use_container_width=True)


def display_data_tab(filtered_df: pd.DataFrame):
    st.markdown('<div class="section-header">📋 数据明细</div>', unsafe_allow_html=True)
    
    if filtered_df.empty:
        st.warning("当前筛选条件下没有数据")
        return
    
    display_cols = []
    for col in ['订单日期', '地区', '类别', '产品名', '品牌', '销售额', '利润', '利润率', '数量', '销售人员']:
        if col in filtered_df.columns:
            display_cols.append(col)
    
    display_df = filtered_df[display_cols].copy()
    
    if '订单日期' in display_df.columns:
        display_df['订单日期'] = display_df['订单日期'].dt.strftime('%Y-%m-%d')
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            '销售额': st.column_config.NumberColumn('销售额', format="¥%.2f"),
            '利润': st.column_config.NumberColumn('利润', format="¥%.2f"),
        }
    )
    
    st.markdown(f"**共 {len(filtered_df)} 条记录**")
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        csv_data = to_csv(display_df)
        st.download_button(
            label="📥 导出当前数据",
            data=csv_data,
            file_name=f"sales_data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        if '地区' in filtered_df.columns or '类别' in filtered_df.columns:
            summary_type = st.selectbox("汇总方式", ["按地区", "按类别", "按销售人员", "按月度"])
            
            group_col = None
            if summary_type == "按地区" and '地区' in filtered_df.columns:
                group_col = '地区'
            elif summary_type == "按类别" and '类别' in filtered_df.columns:
                group_col = '类别'
            elif summary_type == "按销售人员" and '销售人员' in filtered_df.columns:
                group_col = '销售人员'
            elif summary_type == "按月度" and '年月' in filtered_df.columns:
                group_col = '年月'
            
            if group_col:
                processor = st.session_state.processor
                summary_df = processor.analyze_by_dimension(filtered_df, group_col)
                
                if not summary_df.empty:
                    csv_summary = to_csv(summary_df)
                    st.download_button(
                        label="📥 导出汇总数据",
                        data=csv_summary,
                        file_name=f"sales_summary_{group_col}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )


def display_analysis_dashboard():
    processor = st.session_state.processor
    chart_generator = st.session_state.chart_generator
    
    unique_values = processor.get_unique_values()
    
    with st.sidebar:
        st.header("🔍 数据筛选")
        
        filter_options = {}
        
        if 'date_range' in unique_values:
            min_date, max_date = unique_values['date_range']
            date_range = st.date_input(
                "选择日期范围",
                value=(min_date.date(), max_date.date()),
                min_value=min_date.date(),
                max_value=max_date.date()
            )
            if len(date_range) == 2:
                filter_options['date_range'] = date_range
        
        if 'years' in unique_values and len(unique_values['years']) > 1:
            selected_years = st.multiselect(
                "选择年份",
                options=unique_values['years'],
                default=[]
            )
            if selected_years:
                filter_options['years'] = selected_years
        
        if 'regions' in unique_values:
            selected_regions = st.multiselect(
                "选择地区",
                options=unique_values['regions'],
                default=[]
            )
            if selected_regions:
                filter_options['regions'] = selected_regions
        
        if 'categories' in unique_values:
            selected_categories = st.multiselect(
                "选择类别",
                options=unique_values['categories'],
                default=[]
            )
            if selected_categories:
                filter_options['categories'] = selected_categories
        
        if 'salespeople' in unique_values:
            selected_salespeople = st.multiselect(
                "选择销售人员",
                options=unique_values['salespeople'],
                default=[]
            )
            if selected_salespeople:
                filter_options['salespeople'] = selected_salespeople
        
        if 'brands' in unique_values:
            selected_brands = st.multiselect(
                "选择品牌",
                options=unique_values['brands'],
                default=[]
            )
            if selected_brands:
                filter_options['brands'] = selected_brands
        
        st.markdown("---")
        st.markdown("### 💡 提示")
        st.markdown("不选择任何筛选条件时，将显示全部数据。")
        
        if st.button("🔄 重置筛选", use_container_width=True):
            st.rerun()
    
    filtered_df = processor.filter_data(**filter_options)
    
    metrics = processor.calculate_metrics(filtered_df)
    target_info = processor.calculate_target_achievement(filtered_df)
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 整体概览", 
        "🔍 多维度分析", 
        "📦 产品分析", 
        "⚠️ 异常检测",
        "📋 数据明细"
    ])
    
    with tab1:
        display_overview_tab(filtered_df, metrics, target_info)
    
    with tab2:
        display_dimension_tab(filtered_df)
    
    with tab3:
        display_products_tab(filtered_df)
    
    with tab4:
        display_anomalies_tab(filtered_df)
    
    with tab5:
        display_data_tab(filtered_df)


def main():
    st.markdown('<h1 class="main-header">📊 销售数据分析看板</h1>', unsafe_allow_html=True)
    
    init_session_state()
    
    if not st.session_state.data_ready:
        display_upload_section()
        
        st.markdown("---")
        st.markdown("### 💡 快速开始")
        st.markdown("""
        1. **上传订单数据**（必需）：包含订单日期、销售额等核心交易数据
        2. **可选上传商品数据**：用于关联计算成本和利润
        3. **可选上传目标数据**：用于目标达成分析
        4. 点击"开始数据处理"按钮
        5. 使用左侧筛选条件进行多维度分析
        """)
        
        return
    
    display_analysis_dashboard()
    
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 重新上传数据", use_container_width=True):
        st.session_state.processor = AdvancedDataProcessor()
        st.session_state.data_ready = False
        st.rerun()


if __name__ == "__main__":
    main()
