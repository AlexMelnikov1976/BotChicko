# forecast.py

from datetime import datetime
import calendar
import pandas as pd
# from main import get_management_percent, get_management_value, format_ruble
from utils import get_management_percent, get_management_value, format_ruble

# Временно, если функции нет — простая заглушка:
def get_manager_bonus_line(profit_after_usn, format_ruble):
    return ""

def forecast(df):
    """Прогноз по месяцу: учитывает все основные затраты и прибыль."""
    now = datetime.now()  # Текущая дата

    # Оставляем только строки за текущий месяц и год
    current_month_df = df[(df["Дата"].dt.year == now.year) & (df["Дата"].dt.month == now.month)]
    if current_month_df.empty:
        return "⚠️ Нет данных за текущий месяц."

    total_revenue_series = current_month_df["Выручка бар"] + current_month_df["Выручка кухня"]
    salary_series = current_month_df["Начислено"]
    avg_daily_revenue = total_revenue_series.mean()
    avg_daily_salary = salary_series.mean()
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    forecast_revenue = avg_daily_revenue * days_in_month

    fixed_salaries = get_management_value("ЗП упр", "Сумма")
    salary_msg = ""
    if fixed_salaries is None:
        fixed_salaries = 0
        salary_msg = "❗ Не удалось получить фикс. зарплату из управляющей таблицы.\n"
    forecast_salary = avg_daily_salary * days_in_month + fixed_salaries
    labor_cost_share = (forecast_salary / forecast_revenue * 100) if forecast_revenue else 0

    franchise_percent = get_management_percent("Франшиза")
    if franchise_percent is None:
        fc_msg = "❗ Не удалось получить процент по франшизе.\n"
        forecast_franchise = 0
    else:
        forecast_franchise = forecast_revenue * (franchise_percent / 100)
        fc_msg = ""

    writeoff_percent = get_management_percent("Процент списания")
    wo_msg = ""
    if writeoff_percent is None:
        wo_msg = "❗ Не удалось получить процент списания.\n"
        forecast_writeoff = 0
    else:
        forecast_writeoff = forecast_revenue * (writeoff_percent / 1000)

    hozy_percent = get_management_percent("Процент хозы")
    if hozy_percent is None:
        hozy_percent = get_management_percent("Хозы")  # fallback
    hozy_msg = ""
    if hozy_percent is None:
        hozy_msg = "❗ Не удалось получить процент хозрасходов.\n"
        forecast_hozy = 0
    else:
        forecast_hozy = forecast_revenue * (hozy_percent / 100)

    foodcost_month_raw = current_month_df["Фудкост общий, %"]
    foodcost_month_nums = pd.to_numeric(foodcost_month_raw, errors="coerce")
    foodcost_month = foodcost_month_nums.mean()
    forecast_foodcost = forecast_revenue * (foodcost_month / 1000)

    delivery_col_candidates = [col for col in current_month_df.columns if "достав" in col.lower()]
    if delivery_col_candidates:
        delivery_col = delivery_col_candidates[0]
    else:
        raise Exception("Столбец доставки не найден!")
    delivery_series = current_month_df[delivery_col]
    avg_daily_delivery = delivery_series.mean()
    forecast_delivery = avg_daily_delivery * days_in_month
    delivery_percent = get_management_percent("Процент доставка")
    if delivery_percent is not None and delivery_percent > 100:
        delivery_percent = delivery_percent / 100
    if delivery_percent is None:
        delivery_msg = "❗ Не удалось получить процент по доставке.\n"
        forecast_delivery_expense = 0
    else:
        forecast_delivery_expense = forecast_delivery * (delivery_percent / 100)
        delivery_msg = ""

    acquiring_percent = get_management_percent("Эквайринг")
    if acquiring_percent is not None and acquiring_percent > 100:
        acquiring_percent = acquiring_percent / 1000
    if acquiring_percent is None:
        acquiring_msg = "❗ Не удалось получить процент эквайринга.\n"
        forecast_acquiring = 0
    else:
        forecast_acquiring = forecast_revenue * (acquiring_percent / 1000)
        acquiring_msg = ""

    bank_commission_percent = get_management_percent("Комиссия Банка")
    if bank_commission_percent is not None and bank_commission_percent > 100:
        bank_commission_percent = bank_commission_percent / 1000
    if bank_commission_percent is None:
        bank_commission_msg = "❗ Не удалось получить процент комиссии банка.\n"
        forecast_bank_commission = 0
    else:
        forecast_bank_commission = forecast_revenue * (bank_commission_percent / 1000)
        bank_commission_msg = ""

    permanent_costs = get_management_value("Постоянные", "Сумма")
    if permanent_costs is None:
        permanent_costs = 0
        permanent_msg = "❗ Не удалось получить значение постоянных расходов.\n"
    else:
        permanent_msg = ""

    salary_tax_percent = get_management_percent("Налоги ЗП")
    if salary_tax_percent is not None:
        forecast_salary_tax = forecast_salary * (salary_tax_percent / 100)
        salary_tax_msg = ""
    else:
        forecast_salary_tax = 0
        salary_tax_msg = "❗ Не удалось получить процент по налогам ЗП.\n"

    var_expense_share = (forecast_franchise / forecast_revenue * 100) if forecast_revenue else 0
    wo_share = (forecast_writeoff / forecast_revenue * 100) if forecast_revenue else 0
    hozy_share = (forecast_hozy / forecast_revenue * 100) if forecast_revenue else 0

    total_costs = (
        forecast_salary
        + forecast_foodcost
        + forecast_franchise
        + forecast_writeoff
        + forecast_hozy
        + forecast_delivery_expense
        + forecast_acquiring
        + forecast_bank_commission
        + permanent_costs
        + forecast_salary_tax
    )

    profit = forecast_revenue - total_costs

    usn_percent = get_management_percent("УСН")
    if usn_percent is not None:
        forecast_usn = profit * (usn_percent / 100)
        usn_msg = ""
    else:
        forecast_usn = 0
        usn_msg = "❗ Не удалось получить процент УСН.\n"
        
    profit_after_usn = profit - forecast_usn
    bonus_line = get_manager_bonus_line(profit_after_usn, format_ruble)

    return (
        f"📅 Прогноз на {now.strftime('%B %Y')}:\n"
        f"📊 Выручка: {format_ruble(forecast_revenue)}\n"
        f"🪑 ЗП: {format_ruble(forecast_salary)} (LC: {labor_cost_share:.1f}%)\n"
        f"🍔 Фудкост: {format_ruble(forecast_foodcost)} ({foodcost_month/10 :.1f}%)\n"
        f"💼 Франшиза: {format_ruble(forecast_franchise)} ({var_expense_share:.1f}%)\n"
        f"📉 Списание: {format_ruble(forecast_writeoff)} ({wo_share:.1f}%)\n"
        f"🧹 Хозы: {format_ruble(forecast_hozy)} ({hozy_share:.1f}%)\n"
        f"🚚 Доставка: {format_ruble(forecast_delivery_expense)} ({delivery_percent if delivery_percent is not None else '-'}%)\n"
        f"🏦 Эквайринг: {format_ruble(forecast_acquiring)} ({acquiring_percent/10:.1f}%)\n"
        f"💳 Комиссия банка: {format_ruble(forecast_bank_commission)} ({bank_commission_percent/10:.1f}%)\n"
        f"🧾 Налоги на ЗП: {format_ruble(forecast_salary_tax)} ({salary_tax_percent if salary_tax_percent is not None else '-'}%)\n"
        f"🧱 Постоянные: {format_ruble(permanent_costs)}\n"
        f"💰 Прогнозная прибыль: {format_ruble(profit)}\n"
        f"🏛 УСН: {format_ruble(forecast_usn)} ({usn_percent if usn_percent is not None else '-'}%)\n"
        f"💵 Прибыль после УСН: {format_ruble(profit_after_usn)}\n"
        f"{bonus_line}\n"
        f"{fc_msg}{wo_msg}{hozy_msg}{salary_msg}{delivery_msg}{acquiring_msg}{bank_commission_msg}{permanent_msg}"
    )
