from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from anhui_model import (
    ModelParams,
    find_top_configs,
    load_device_specs,
    load_pivot_from_workbook,
    load_uploaded_pivot,
)


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_WORKBOOK = BASE_DIR / "版本号3.6-安徽容量测算模型（314电芯）.xlsx"


st.set_page_config(page_title="安徽省储能配置测算", layout="wide")

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.4rem; padding-bottom: 2rem;}
    h1 {font-size: 1.8rem; letter-spacing: 0;}
    h2, h3 {letter-spacing: 0;}
    [data-testid="stMetricValue"] {font-size: 1.4rem;}
    .stDataFrame {border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False)
def cached_default_data(path: str):
    pivot = load_pivot_from_workbook(path)
    specs = load_device_specs(path)
    return pivot, specs


@st.cache_data(show_spinner=False)
def cached_uploaded_data(file):
    return load_uploaded_pivot(file)


st.title("安徽省储能最优配置测算")

with st.sidebar:
    st.header("数据")
    uploaded = st.file_uploader("负荷数据或模型文件", type=["xlsx", "xlsm", "csv"])
    use_default = uploaded is None

    st.header("筛选条件")
    unit_range = st.slider("台数范围", 1, 20, (1, 8), step=1)
    run_days_range = st.slider("折算运行天数", 0, 365, (300, 320), step=1)
    payback_range = st.slider("静态回收期（年）", 0.0, 16.0, (2.0, 6.0), step=0.1)

    st.header("设备")
    selected_models = st.multiselect("设备型号", ["S1", "S2", "X3"], default=["S1", "S2", "X3"])
    selected_modes = st.multiselect("充放模式", [1, 2], default=[1, 2], format_func=lambda x: f"{x}充放")

    st.header("商务参数")
    discount_rate = st.number_input("折扣", min_value=0.0, max_value=1.0, value=0.8, step=0.01)
    brokerage_rate = st.number_input("居间费率", min_value=0.0, max_value=1.0, value=0.13, step=0.01)

    with st.expander("固定参数调整", expanded=False):
        transformer_capacity = st.number_input("变压器容量(kVA)", min_value=1.0, value=12550.0, step=50.0)
        safety_factor = st.number_input("安全系数", min_value=0.0, max_value=1.0, value=0.85, step=0.01)
        morning_limit_ratio = st.number_input("上午限制放电量比例", min_value=0.0, max_value=2.0, value=1.0, step=0.05)
        afternoon_limit_ratio = st.number_input("下午限制放电量比例", min_value=0.0, max_value=2.0, value=1.0, step=0.05)
        afternoon_charge_ratio = st.number_input("下午平段充电倍率", min_value=0.0, max_value=2.0, value=0.0, step=0.05)
        annual_decay_rate = st.number_input("年衰减率", min_value=0.0, max_value=0.2, value=0.025, step=0.001, format="%.3f")
        efficiency = st.number_input("回收期系统效率", min_value=0.0, max_value=1.0, value=0.865, step=0.005)
        operation_cost_rate = st.number_input("运维成本率", min_value=0.0, max_value=1.0, value=0.01, step=0.005)
        tax_rate = st.number_input("所得税率", min_value=0.0, max_value=1.0, value=0.05, step=0.005)
        interest_rate = st.number_input("年利率", min_value=0.0, max_value=1.0, value=0.04, step=0.005)

try:
    if use_default:
        pivot_df, specs = cached_default_data(str(DEFAULT_WORKBOOK))
        data_source = DEFAULT_WORKBOOK.name
    else:
        pivot_df = cached_uploaded_data(uploaded)
        specs = load_device_specs(DEFAULT_WORKBOOK)
        data_source = uploaded.name
except Exception as exc:
    st.error(f"数据读取失败：{exc}")
    st.stop()

left, mid, right = st.columns(3)
left.metric("数据源", data_source)
mid.metric("负荷天数", f"{len(pivot_df)} 天")
right.metric("设备组合", f"{len(specs)} 组")

run_clicked = st.button("开始测算", type="primary", use_container_width=True)
if not run_clicked and "top_df" not in st.session_state:
    st.info("设置左侧条件后点击开始测算，页面会输出符合要求的前 10 个配置。")
    st.stop()

if run_clicked:
    params = ModelParams(
        safety_factor=safety_factor,
        transformer_capacity_kva=transformer_capacity,
        discount_rate=discount_rate,
        brokerage_rate=brokerage_rate,
        morning_limit_ratio=morning_limit_ratio,
        afternoon_limit_ratio=afternoon_limit_ratio,
        afternoon_charge_ratio=afternoon_charge_ratio,
        annual_decay_rate=annual_decay_rate,
        system_efficiency_payback=efficiency,
        operation_cost_rate=operation_cost_rate,
        tax_rate=tax_rate,
        interest_rate=interest_rate,
    )
    with st.spinner("正在扫描候选配置..."):
        top_df, details = find_top_configs(
            pivot_df=pivot_df,
            specs=specs,
            unit_range=unit_range,
            run_days_range=run_days_range,
            payback_range=payback_range,
            params=params,
            selected_models=selected_models,
            selected_modes=selected_modes,
            limit=10,
        )
    st.session_state["top_df"] = top_df
    st.session_state["details"] = details
else:
    top_df = st.session_state["top_df"]
    details = st.session_state["details"]

if top_df.empty:
    st.warning("当前条件下没有符合要求的配置。可以放宽运行天数或回收期范围，再重新查看。")
    st.stop()

display_df = top_df.copy()
num_cols = display_df.select_dtypes(include="number").columns
display_df[num_cols] = display_df[num_cols].round(3)

st.subheader("符合要求的前 10 个配置")
st.dataframe(display_df, use_container_width=True, hide_index=True)

csv = top_df.to_csv(index=False).encode("utf-8-sig")
st.download_button("下载结果 CSV", data=csv, file_name="安徽储能最优配置_top10.csv", mime="text/csv")

st.subheader("配置详情")
keys = [f"{row['设备型号']}-{int(row['充放模式'])}-{int(row['台数'])}" for _, row in top_df.iterrows()]
selected_key = st.selectbox("选择配置", keys, format_func=lambda key: f"{key.split('-')[0]} / {key.split('-')[1]}充放 / {key.split('-')[2]}台")
detail = details[selected_key]
selected_result = detail["result"]

a, b, c, d = st.columns(4)
a.metric("折算运行天数", f"{selected_result.run_days:.2f}")
b.metric("静态回收期", f"{selected_result.payback_years:.2f} 年" if selected_result.payback_years is not None else "未回收")
c.metric("放电均价", f"{selected_result.discharge_price:.4f}")
d.metric("充电均价", f"{selected_result.charge_price:.4f}")

tab_monthly, tab_cash, tab_payback = st.tabs(["月度充放电", "现金流", "回收期明细表"])
with tab_monthly:
    monthly = detail["monthly"].copy()
    for col in monthly.select_dtypes(include="number").columns:
        monthly[col] = monthly[col].round(3)
    st.dataframe(monthly, use_container_width=True, hide_index=True)

with tab_cash:
    payback = detail["payback"]
    cash_df = pd.DataFrame(
        {
            "年份": list(range(17)),
            "充电量(万度)": payback["charge_kwh_10k"],
            "放电量(万度)": payback["discharge_kwh_10k"],
            "电费收入(万元)": payback["cash_in_wan"],
            "间接法累计现金流(万元)": payback["indirect_cum_cash_flow_wan"],
        }
    )
    st.line_chart(cash_df.set_index("年份")[["间接法累计现金流(万元)"]])
    st.dataframe(cash_df.round(3), use_container_width=True, hide_index=True)

with tab_payback:
    payback_table = detail["payback"]["payback_table"].copy()
    payback = detail["payback"]
    audit_df = pd.DataFrame(
        {
            "字段": [
                "折算运行天数",
                "回收期系统效率",
                "充放模式",
                "第1年电池容量系数",
                "第1年充电量(万度)",
                "第1年放电量(万度)",
                "放电均价(元/kWh)",
                "充电均价(元/kWh)",
                "折扣",
                "第1年电费收入(万元)",
            ],
            "数值": [
                selected_result.run_days,
                efficiency,
                selected_result.mode,
                payback["battery_ratio"][1],
                payback["charge_kwh_10k"][1],
                payback["discharge_kwh_10k"][1],
                selected_result.discharge_price,
                selected_result.charge_price,
                discount_rate,
                payback["cash_in_wan"][1],
            ],
        }
    )
    audit_df["数值"] = audit_df["数值"].apply(lambda value: round(value, 6) if isinstance(value, float) else value)
    st.dataframe(audit_df, use_container_width=True, hide_index=True)
    payback_table = payback_table.map(lambda value: round(value, 4) if isinstance(value, float) else value)
    st.dataframe(payback_table, use_container_width=True, hide_index=True)
