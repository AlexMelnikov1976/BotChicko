from datetime import datetime, timedelta
import calendar
import pandas as pd
from utils import get_management_percent, get_management_value, format_ruble

def get_manager_bonus_line(profit_after_usn, format_ruble):
    # Место для вашей логики по бонусу, если нужно
    return ""

def forecast(df):
    """Прогноз по текущему месяцу: учитывает все основные затраты и прибыль."""
    now = datetime.now()
    # Только строки за текущий месяц и год
    current_month_df = df[(df["Дата"].dt.year == now.year) & (df["Дата"].dt.month == now.month)]
    if current_month_df.empty:
        return "⚠️ Нет данных за текущий месяц."

    return _forecast_core(current_month_df, now.year, now.month, period_label=f"Прогноз на {now.strftime('%B %Y')}")

def forecast_for_period(df, period='current'):
    """Прогноз по выбранному месяцу: period='current' или 'previous'."""
    today = datetime.now()
    if period == 'current':
        year = today.year
        month = today.month
    elif period == 'previous':
        first_day_this_month = today.replace(day=1)
        last_day_prev_month = first_day_this_month - timedelta(days=1)
        year = last_day_prev_month.year
        month = last_day_prev_month.month
    else:
        return "❌ Некорректный период. Используйте 'current' или 'previous'."

    period_df = df[(df["Дата"].dt.year == year) & (df["Дата"].dt.month == month)]
    if period_df.empty:
        period_text = "текущий" if period == "current" else "прошлый"
        return f"⚠️ Нет данных за {period_text} месяц."
    
    label = f"Итоги за {datetime(year, month, 1).strftime('%B %Y')}"
    return _forecast_core(period_df, year, month, period_label=label)

def _forecast_core(df_period, year, month, period_label="Прогноз"):
    # Суммарная выручка
    total_revenue_series = df_period["Выручка бар"] + df_period["Выручка кухня"]
    salary_series = df_period["Начислено"]

    avg_daily_revenue = total_revenue_series.mean()
    avg_daily_salary = salary_series.mean()
    days_in_month = calendar.monthrange(year, month)[1]

    # Для текущего месяца — прогноз, для завершенного — сумма (переиспользуем ядро)
    # Здесь — итог за период (месяц)
    total_revenue = total_revenue_series.sum()

    fixed_salaries = get_management_value("ЗП упр", "Сумма")
    salary_msg = ""
    if fixed_salaries is None:
        fixed_salaries = 0
        salary_msg = "❗ Не удалось получить фикс. зарплату из управляющей таблицы.\n"
    total_salary = salary_series.sum() + fixed_salaries
    labor_cost_share = (total_salary / total_revenue * 100) if total_revenue else 0

    franchise_percent = get_management_percent("Франшиза")
    if franchise_percent is None:
        fc_msg = "❗ Не удалось получить процент по франшизе.\n"
        franchise_expense = 0
    else:
        franchise_expense = total_revenue * (franchise_percent / 100)
        fc_msg = ""

    writeoff_percent = get_management_percent("Процент списания")
    wo_msg = ""
    if writeoff_percent is None:
        wo_msg = "❗ Не удалось получить процент списания.\n"
        writeoff_expense = 0
    else:
        writeoff_expense = total_revenue * (writeoff_percent / 1000)

    hozy_percent = get_management_percent("Процент хозы")
    if hozy_percent is None:
        hozy_percent = get_management_percent("Хозы")
    hozy_msg = ""
    if hozy_percent is None:
        hozy_msg = "❗ Не удалось получить процент хозрасходов.\n"
        hozy_expense = 0
    else:
        hozy_expense = total_revenue * (hozy_percent / 100)

    foodcost_month_raw = df_period["Фудкост общий, %"]
    foodcost_month_nums = pd.to_numeric(foodcost_month_raw, errors="coerce")
    foodcost_month = foodcost_month_nums.mean()
    foodcost_expense = total_revenue * (foodcost_month / 1000)

    delivery_col_candidates = [col for col in df_period.columns if "достав" in col.lower()]
    if delivery_col_candidates:
        delivery_col = delivery_col_candidates[0]
    else:
        return "Столбец доставки не найден!"
    delivery_series = df_period[delivery_col]
    total_delivery = delivery_series.sum()
    delivery_percent = get_management_percent("Процент доставка")
    if delivery_percent is not None and delivery_percent > 100:
        delivery_percent = delivery_percent / 100
    if delivery_percent is None:
        delivery_msg = "❗ Не удалось получить процент по доставке.\n"
        delivery_expense = 0
    else:
        delivery_expense = total_delivery * (delivery_percent / 100)
        delivery_msg = ""

    acquiring_percent = get_management_percent("Эквайринг")
    if acquiring_percent is not None and acquiring_percent > 100:
        acquiring_percent = acquiring_percent / 1000
    if acquiring_percent is None:
        acquiring_msg = "❗ Не удалось получить процент эквайринга.\n"
        acquiring_expense = 0
    else:
        acquiring_expense = total_revenue * (acquiring_percent / 1000)
        acquiring_msg = ""

    bank_commission_percent = get_management_percent("Комиссия Банка")
    if bank_commission_percent is not None and bank_commission_percent > 100:
        bank_commission_percent = bank_commission_percent / 1000
    if bank_commission_percent is None:
        bank_commission_msg = "❗ Не удалось получить процент комиссии банка.\n"
        bank_commission_expense = 0
    else:
        bank_commission_expense = total_revenue * (bank_commission_percent / 1000)
        bank_commission_msg = ""

    permanent_costs = get_management_value("Постоянные", "Сумма")
    if permanent_costs is None:
        permanent_costs = 0
        permanent_msg = "❗ Не удалось получить значение постоянных расходов.\n"
    else:
        permanent_msg = ""

    salary_tax_percent = get_management_percent("Налоги ЗП")
    if salary_tax_percent is not None:
        salary_tax = total_salary * (salary_tax_percent / 100)
        salary_tax_msg = ""
    else:
        salary_tax = 0
        salary_tax_msg = "❗ Не удалось получить процент по налогам ЗП.\n"

    var_expense_share = (franchise_expense / total_revenue * 100) if total_revenue else 0
    wo_share = (writeoff_expense / total_revenue * 100) if total_revenue else 0
    hozy_share = (hozy_expense / total_revenue * 100) if total_revenue else 0

    total_costs = (
        total_salary
        + foodcost_expense
        + franchise_expense
        + writeoff_expense
        + hozy_expense
        + delivery_expense
        + acquiring_expense
        + bank_commission_expense
        + permanent_costs
        + salary_tax
    )

    profit = total_revenue - total_costs

    usn_percent = get_management_percent("УСН")
    if usn_percent is not None:
        usn_expense = profit * (usn_percent / 100)
        usn_msg = ""
    else:
        usn_expense = 0
        usn_msg = "❗ Не удалось получить процент УСН.\n"

    profit_after_usn = profit - usn_expense
    bonus_line = get_manager_bonus_line(profit_after_usn, format_ruble)

    return (
        f"📅 {period_label}:\n"
        f"📊 Выручка: {format_ruble(total_revenue)}\n"
        f"🪑 ЗП: {format_ruble(total_salary)} (LC: {labor_cost_share:.1f}%)\n"
        f"🍔 Фудкост: {format_ruble(foodcost_expense)} ({foodcost_month/10:.1f}%)\n"
        f"💼 Франшиза: {format_ruble(franchise_expense)} ({var_expense_share:.1f}%)\n"
        f"📉 Списание: {format_ruble(writeoff_expense)} ({wo_share:.1f}%)\n"
        f"🧹 Хозы: {format_ruble(hozy_expense)} ({hozy_share:.1f}%)\n"
        f"🚚 Доставка: {format_ruble(delivery_expense)} ({delivery_percent if delivery_percent is not None else '-' }%)\n"
        f"🏦 Эквайринг: {format_ruble(acquiring_expense)} ({acquiring_percent/10 if acquiring_percent is not None else '-'}%)\n"
        f"💳 Комиссия банка: {format_ruble(bank_commission_expense)} ({bank_commission_percent/10 if bank_commission_percent is not None else '-'}%)\n"
        f"🧾 Налоги на ЗП: {format_ruble(salary_tax)} ({salary_tax_percent if salary_tax_percent is not None else '-'}%)\n"
        f"🧱 Постоянные: {format_ruble(permanent_costs)}\n"
        f"💰 Прибыль: {format_ruble(profit)}\n"
        f"🏛 УСН: {format_ruble(usn_expense)} ({usn_percent if usn_percent is not None else '-'}%)\n"
        f"💵 Прибыль после УСН: {format_ruble(profit_after_usn)}\n"
        f"{bonus_line}\n"
        f"{fc_msg}{wo_msg}{hozy_msg}{salary_msg}{delivery_msg}{acquiring_msg}{bank_commission_msg}{permanent_msg}{salary_tax_msg}{usn_msg}"
    )
