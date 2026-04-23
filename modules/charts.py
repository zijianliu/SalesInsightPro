import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Optional, List, Dict, Any


class ChartGenerator:
    def __init__(self, color_theme: str = 'plotly'):
        self.color_theme = color_theme
        self.colors = px.colors.qualitative.Plotly
        self.primary_color = self.colors[0]
        self.secondary_color = self.colors[2]
        self.success_color = '#28a745'
        self.warning_color = '#ffc107'
        self.danger_color = '#dc3545'

    def sales_trend_chart(self, df: pd.DataFrame, freq: str = 'M') -> Optional[go.Figure]:
        if df.empty:
            return None

        df_trend = df.copy()
        
        if freq == 'M':
            df_trend['period'] = df_trend['年月']
        elif freq == 'Q':
            df_trend['period'] = df_trend.apply(lambda x: f"{x['年份']}Q{x['季度']}", axis=1)
        else:
            df_trend['period'] = df_trend['年月']
        
        trend_data = df_trend.groupby('period').agg({
            '销售额': 'sum',
            '利润': 'sum'
        }).reset_index()
        
        trend_data['销售额环比'] = trend_data['销售额'].pct_change() * 100
        trend_data['利润环比'] = trend_data['利润'].pct_change() * 100
        
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            row_heights=[0.7, 0.3]
        )
        
        fig.add_trace(go.Scatter(
            x=trend_data['period'],
            y=trend_data['销售额'],
            mode='lines+markers+text',
            name='销售额',
            line=dict(color=self.primary_color, width=3),
            marker=dict(size=10),
            text=[f'¥{v/10000:.0f}万' if v >= 10000 else f'¥{v:.0f}' for v in trend_data['销售额']],
            textposition='top center',
        ), row=1, col=1)
        
        fig.add_trace(go.Scatter(
            x=trend_data['period'],
            y=trend_data['利润'],
            mode='lines+markers',
            name='利润',
            line=dict(color=self.secondary_color, width=2, dash='dot'),
            marker=dict(size=8),
        ), row=1, col=1)
        
        colors = []
        for pct in trend_data['销售额环比'].fillna(0):
            if pct >= 0:
                colors.append(self.success_color)
            else:
                colors.append(self.danger_color)
        
        fig.add_trace(go.Bar(
            x=trend_data['period'],
            y=trend_data['销售额环比'],
            name='销售额环比(%)',
            marker_color=colors,
            text=[f'{v:.1f}%' if pd.notna(v) else '' for v in trend_data['销售额环比']],
            textposition='auto',
        ), row=2, col=1)
        
        fig.update_layout(
            title='销售趋势分析',
            showlegend=True,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
            template='plotly_white',
            height=500,
        )
        
        fig.update_yaxes(title_text='金额', row=1, col=1)
        fig.update_yaxes(title_text='环比(%)', row=2, col=1)
        
        return fig

    def region_comparison_chart(self, df: pd.DataFrame) -> Optional[go.Figure]:
        if df.empty or '地区' not in df.columns:
            return None

        region_data = df.groupby('地区').agg({
            '销售额': 'sum',
            '利润': 'sum'
        }).reset_index()
        
        region_data = region_data.sort_values('销售额', ascending=False)
        
        total_sales = region_data['销售额'].sum()
        region_data['销售额占比'] = region_data['销售额'] / total_sales * 100
        
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{'type': 'xy'}, {'type': 'domain'}]],
            column_widths=[0.6, 0.4]
        )
        
        fig.add_trace(go.Bar(
            x=region_data['地区'],
            y=region_data['销售额'],
            name='销售额',
            marker_color=self.primary_color,
            text=[f'¥{v/10000:.0f}万' if v >= 10000 else f'¥{v:.0f}' for v in region_data['销售额']],
            textposition='auto',
        ), row=1, col=1)
        
        fig.add_trace(go.Bar(
            x=region_data['地区'],
            y=region_data['利润'],
            name='利润',
            marker_color=self.secondary_color,
            text=[f'¥{v/10000:.0f}万' if v >= 10000 else f'¥{v:.0f}' for v in region_data['利润']],
            textposition='auto',
        ), row=1, col=1)
        
        fig.add_trace(go.Pie(
            labels=region_data['地区'],
            values=region_data['销售额'],
            name='销售额占比',
            marker_colors=self.colors,
            textinfo='percent+label',
            textposition='inside',
            hole=0.4,
        ), row=1, col=2)
        
        fig.update_layout(
            title='地区销售对比',
            showlegend=False,
            barmode='group',
            template='plotly_white',
            height=450,
        )
        
        return fig

    def category_analysis_chart(self, df: pd.DataFrame) -> Optional[go.Figure]:
        if df.empty or '类别' not in df.columns:
            return None

        category_data = df.groupby('类别').agg({
            '销售额': 'sum',
            '利润': 'sum'
        }).reset_index()
        
        category_data['利润率'] = category_data['利润'] / category_data['销售额'] * 100
        category_data = category_data.sort_values('销售额', ascending=False)
        
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            row_heights=[0.6, 0.4]
        )
        
        fig.add_trace(go.Bar(
            x=category_data['类别'],
            y=category_data['销售额'],
            name='销售额',
            marker_color=self.primary_color,
            text=[f'¥{v/10000:.0f}万' if v >= 10000 else f'¥{v:.0f}' for v in category_data['销售额']],
            textposition='auto',
        ), row=1, col=1)
        
        fig.add_trace(go.Scatter(
            x=category_data['类别'],
            y=category_data['利润率'],
            mode='lines+markers',
            name='利润率(%)',
            line=dict(color=self.warning_color, width=2),
            marker=dict(size=10),
            text=[f'{v:.1f}%' for v in category_data['利润率']],
            textposition='top center',
        ), row=2, col=1)
        
        fig.update_layout(
            title='类别分析 - 销售额与利润率',
            showlegend=True,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
            template='plotly_white',
            height=450,
        )
        
        fig.update_yaxes(title_text='销售额', row=1, col=1)
        fig.update_yaxes(title_text='利润率(%)', row=2, col=1)
        
        return fig

    def product_performance_chart(self, df: pd.DataFrame, top_n: int = 10) -> Optional[go.Figure]:
        if df.empty or '产品名' not in df.columns:
            return None

        product_data = df.groupby('产品名').agg({
            '销售额': 'sum',
            '利润': 'sum'
        }).reset_index()
        
        product_data['利润率'] = product_data['利润'] / product_data['销售额'] * 100
        
        top_products = product_data.sort_values('销售额', ascending=False).head(top_n)
        top_products = top_products.sort_values('销售额', ascending=True)
        
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{'type': 'xy'}, {'type': 'xy'}]],
            column_widths=[0.5, 0.5]
        )
        
        fig.add_trace(go.Bar(
            y=top_products['产品名'],
            x=top_products['销售额'],
            name='销售额',
            orientation='h',
            marker_color=self.primary_color,
            text=[f'¥{v/10000:.1f}万' if v >= 10000 else f'¥{v:.0f}' for v in top_products['销售额']],
            textposition='outside',
        ), row=1, col=1)
        
        fig.add_trace(go.Scatter(
            y=top_products['产品名'],
            x=top_products['利润率'],
            mode='markers',
            name='利润率(%)',
            marker=dict(
                color=top_products['利润率'],
                colorscale='RdYlGn',
                size=15,
                showscale=True
            ),
        ), row=1, col=2)
        
        fig.update_layout(
            title=f'Top {top_n} 产品表现',
            showlegend=False,
            template='plotly_white',
            height=500,
        )
        
        fig.update_xaxes(title_text='销售额', row=1, col=1)
        fig.update_xaxes(title_text='利润率(%)', row=1, col=2)
        
        return fig

    def salesperson_analysis_chart(self, df: pd.DataFrame) -> Optional[go.Figure]:
        if df.empty or '销售人员' not in df.columns:
            return None

        salesperson_data = df.groupby('销售人员').agg({
            '销售额': 'sum',
            '利润': 'sum',
            '订单日期': 'count'
        }).reset_index()
        
        salesperson_data = salesperson_data.rename(columns={'订单日期': '订单数'})
        salesperson_data['客单价'] = salesperson_data['销售额'] / salesperson_data['订单数']
        salesperson_data = salesperson_data.sort_values('销售额', ascending=False)
        
        fig = make_subplots(
            rows=2, cols=2,
            specs=[
                [{'type': 'xy'}, {'type': 'xy'}],
                [{'type': 'xy', 'colspan': 2}, None]
            ],
            vertical_spacing=0.15,
            horizontal_spacing=0.1
        )
        
        fig.add_trace(go.Bar(
            x=salesperson_data['销售人员'],
            y=salesperson_data['销售额'],
            name='销售额',
            marker_color=self.primary_color,
            text=[f'¥{v/10000:.0f}万' if v >= 10000 else f'¥{v:.0f}' for v in salesperson_data['销售额']],
            textposition='auto',
        ), row=1, col=1)
        
        fig.add_trace(go.Bar(
            x=salesperson_data['销售人员'],
            y=salesperson_data['订单数'],
            name='订单数',
            marker_color=self.secondary_color,
            text=[f'{v:.0f}' for v in salesperson_data['订单数']],
            textposition='auto',
        ), row=1, col=2)
        
        fig.add_trace(go.Scatter(
            x=salesperson_data['销售人员'],
            y=salesperson_data['客单价'],
            mode='lines+markers+text',
            name='客单价',
            line=dict(color=self.warning_color, width=2),
            marker=dict(size=10),
            text=[f'¥{v:.0f}' for v in salesperson_data['客单价']],
            textposition='top center',
        ), row=2, col=1)
        
        fig.update_layout(
            title='销售人员业绩分析',
            showlegend=False,
            template='plotly_white',
            height=600,
        )
        
        fig.update_yaxes(title_text='销售额', row=1, col=1)
        fig.update_yaxes(title_text='订单数', row=1, col=2)
        fig.update_yaxes(title_text='客单价', row=2, col=1)
        
        return fig

    def target_achievement_chart(
        self, 
        actual_sales: float, 
        target_sales: float,
        actual_profit: float = 0,
        target_profit: float = 0
    ) -> Optional[go.Figure]:
        if target_sales <= 0:
            return None
        
        achievement_rate = actual_sales / target_sales
        profit_achievement_rate = actual_profit / target_profit if target_profit > 0 else 0
        
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{'type': 'indicator'}, {'type': 'indicator'}]],
            column_widths=[0.5, 0.5]
        )
        
        sales_color = self.success_color if achievement_rate >= 1 else self.danger_color
        fig.add_trace(go.Indicator(
            mode="gauge+number+delta",
            value=actual_sales,
            title={'text': "销售额达成", 'font': {'size': 16}},
            delta={'reference': target_sales, 'relative': True, 'valueformat': '.1%'},
            gauge={
                'axis': {'range': [0, max(actual_sales, target_sales) * 1.2]},
                'bar': {'color': sales_color},
                'steps': [
                    {'range': [0, target_sales * 0.8], 'color': '#f0f0f0'},
                    {'range': [target_sales * 0.8, target_sales], 'color': '#e0e0e0'}
                ],
                'threshold': {
                    'line': {'color': self.primary_color, 'width': 4},
                    'thickness': 0.75,
                    'value': target_sales
                }
            },
            number={'prefix': '¥', 'valueformat': ',.0f'}
        ), row=1, col=1)
        
        if target_profit > 0:
            profit_color = self.success_color if profit_achievement_rate >= 1 else self.danger_color
            fig.add_trace(go.Indicator(
                mode="gauge+number+delta",
                value=actual_profit,
                title={'text': "利润达成", 'font': {'size': 16}},
                delta={'reference': target_profit, 'relative': True, 'valueformat': '.1%'},
                gauge={
                    'axis': {'range': [0, max(actual_profit, target_profit) * 1.2]},
                    'bar': {'color': profit_color},
                    'steps': [
                        {'range': [0, target_profit * 0.8], 'color': '#f0f0f0'},
                        {'range': [target_profit * 0.8, target_profit], 'color': '#e0e0e0'}
                    ],
                    'threshold': {
                        'line': {'color': self.secondary_color, 'width': 4},
                        'thickness': 0.75,
                        'value': target_profit
                    }
                },
                number={'prefix': '¥', 'valueformat': ',.0f'}
            ), row=1, col=2)
        
        fig.update_layout(
            title='目标达成情况',
            template='plotly_white',
            height=350,
        )
        
        return fig

    def actual_vs_target_chart(
        self, 
        trend_data: pd.DataFrame,
        target_data: Optional[pd.DataFrame] = None
    ) -> Optional[go.Figure]:
        if trend_data.empty:
            return None
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=trend_data['period'],
            y=trend_data['销售额'],
            name='实际销售额',
            marker_color=self.primary_color,
            text=[f'¥{v/10000:.0f}万' if v >= 10000 else f'¥{v:.0f}' for v in trend_data['销售额']],
            textposition='auto',
        ))
        
        if target_data is not None and not target_data.empty:
            if '年月' in target_data.columns and '目标销售额' in target_data.columns:
                target_by_period = target_data.groupby('年月')['目标销售额'].sum().reset_index()
                
                fig.add_trace(go.Scatter(
                    x=target_by_period['年月'],
                    y=target_by_period['目标销售额'],
                    mode='lines+markers',
                    name='目标销售额',
                    line=dict(color=self.danger_color, width=2, dash='dash'),
                    marker=dict(size=8),
                ))
        
        fig.update_layout(
            title='实际销售额 vs 目标销售额',
            xaxis_title='期间',
            yaxis_title='金额',
            barmode='group',
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
            template='plotly_white',
            height=400,
        )
        
        return fig

    def anomaly_detection_chart(self, anomalies: List[Dict[str, Any]]) -> Optional[go.Figure]:
        if not anomalies:
            return None
        
        high_anomalies = [a for a in anomalies if a.get('severity') == 'high']
        medium_anomalies = [a for a in anomalies if a.get('severity') == 'medium']
        
        fig = go.Figure()
        
        if high_anomalies:
            fig.add_trace(go.Bar(
                x=[a.get('period', a.get('value', '')) for a in high_anomalies],
                y=[1 for _ in high_anomalies],
                name='高风险',
                marker_color=self.danger_color,
                text=[a.get('message', '') for a in high_anomalies],
                textposition='auto',
                width=0.5
            ))
        
        if medium_anomalies:
            fig.add_trace(go.Bar(
                x=[a.get('period', a.get('value', '')) for a in medium_anomalies],
                y=[1 for _ in medium_anomalies],
                name='中风险',
                marker_color=self.warning_color,
                text=[a.get('message', '') for a in medium_anomalies],
                textposition='auto',
                width=0.5
            ))
        
        fig.update_layout(
            title=f'异常波动检测 (共 {len(anomalies)} 项)',
            showlegend=True,
            barmode='stack',
            yaxis=dict(showticklabels=False, range=[0, 2]),
            template='plotly_white',
            height=300,
        )
        
        return fig

    def category_pie_chart(self, df: pd.DataFrame) -> Optional[go.Figure]:
        if df.empty or '类别' not in df.columns:
            return None

        category_data = df.groupby('类别').agg({
            '销售额': 'sum'
        }).reset_index()
        
        fig = px.pie(
            category_data,
            values='销售额',
            names='类别',
            title='类别销售额占比',
            color_discrete_sequence=self.colors,
            hole=0.4
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>销售额: %{value:,.0f}<br>占比: %{percent}'
        )
        
        fig.update_layout(
            template='plotly_white',
            showlegend=True
        )
        
        return fig

    def top_products_chart(self, df: pd.DataFrame, top_n: int = 10) -> Optional[go.Figure]:
        if df.empty or '产品名' not in df.columns:
            return None

        product_data = df.groupby('产品名').agg({
            '销售额': 'sum',
            '利润': 'sum'
        }).reset_index()
        
        product_data = product_data.sort_values('销售额', ascending=False).head(top_n)
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            y=product_data['产品名'],
            x=product_data['销售额'],
            name='销售额',
            orientation='h',
            marker_color=self.primary_color,
            text=product_data['销售额'].round(0),
            textposition='auto'
        ))
        
        fig.add_trace(go.Bar(
            y=product_data['产品名'],
            x=product_data['利润'],
            name='利润',
            orientation='h',
            marker_color=self.secondary_color,
            text=product_data['利润'].round(0),
            textposition='auto'
        ))
        
        fig.update_layout(
            title=f'Top {top_n} 产品销售排行',
            xaxis_title='金额',
            yaxis_title='产品名',
            yaxis=dict(autorange='reversed'),
            barmode='group',
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
            template='plotly_white'
        )
        
        return fig

    def salesperson_performance_chart(self, df: pd.DataFrame) -> Optional[go.Figure]:
        if df.empty or '销售人员' not in df.columns:
            return None

        salesperson_data = df.groupby('销售人员').agg({
            '销售额': 'sum',
            '利润': 'sum'
        }).reset_index()
        
        salesperson_data = salesperson_data.sort_values('销售额', ascending=False)
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=salesperson_data['销售人员'],
            y=salesperson_data['销售额'],
            name='销售额',
            marker_color=self.primary_color,
            text=salesperson_data['销售额'].round(0),
            textposition='auto'
        ))
        
        fig.add_trace(go.Bar(
            x=salesperson_data['销售人员'],
            y=salesperson_data['利润'],
            name='利润',
            marker_color=self.secondary_color,
            text=salesperson_data['利润'].round(0),
            textposition='auto'
        ))
        
        fig.update_layout(
            title='销售人员业绩对比',
            xaxis_title='销售人员',
            yaxis_title='金额',
            barmode='group',
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
            template='plotly_white'
        )
        
        return fig

    def dimension_comparison_chart(
        self, 
        dimension_data: pd.DataFrame,
        dimension_name: str,
        title: str = ""
    ) -> Optional[go.Figure]:
        if dimension_data.empty:
            return None
        
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{'type': 'xy'}, {'type': 'domain'}]],
            column_widths=[0.6, 0.4]
        )
        
        fig.add_trace(go.Bar(
            x=dimension_data[dimension_name],
            y=dimension_data['销售额'],
            name='销售额',
            marker_color=self.primary_color,
            text=[f'¥{v/10000:.0f}万' if v >= 10000 else f'¥{v:.0f}' for v in dimension_data['销售额']],
            textposition='auto',
        ), row=1, col=1)
        
        fig.add_trace(go.Bar(
            x=dimension_data[dimension_name],
            y=dimension_data['利润'],
            name='利润',
            marker_color=self.secondary_color,
            text=[f'¥{v/10000:.0f}万' if v >= 10000 else f'¥{v:.0f}' for v in dimension_data['利润']],
            textposition='auto',
        ), row=1, col=1)
        
        fig.add_trace(go.Pie(
            labels=dimension_data[dimension_name],
            values=dimension_data['销售额'],
            name='销售额占比',
            marker_colors=self.colors,
            textinfo='percent+label',
            textposition='inside',
            hole=0.4,
        ), row=1, col=2)
        
        fig.update_layout(
            title=title or f'{dimension_name}维度分析',
            showlegend=False,
            barmode='group',
            template='plotly_white',
            height=450,
        )
        
        return fig
