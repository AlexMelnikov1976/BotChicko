... (весь остальной код остаётся без изменений)

# Обработка команды /managers
async def managers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()

        if "Менеджер" not in df.columns:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Колонка 'Менеджер' не найдена в данных.")
            return

        # 🔽 Фильтрация только по текущему месяцу и непустым менеджерам
        now = datetime.now()
        filtered = df[
            df["Менеджер"].notna() &
            (df["Дата"].dt.year == now.year) &
            (df["Дата"].dt.month == now.month)
        ]

        if filtered.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Нет строк с указанными менеджерами за текущий месяц.")
            return

        manager_stats = filtered.groupby("Менеджер").agg({
            "Выручка бар": "sum",
            "Выручка кухня": "sum",
            "Ср. чек общий": "mean",
            "Ср. поз чек общий": "mean"
        }).fillna(0)

        manager_stats["Общая выручка"] = manager_stats["Выручка бар"] + manager_stats["Выручка кухня"]
        top_manager = manager_stats.sort_values("Общая выручка", ascending=False).head(1)

        if top_manager.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Не удалось определить лучшего менеджера.")
            return

        name = str(top_manager.index[0])
        total = top_manager["Общая выручка"].values[0]
        avg_check = top_manager["Ср. чек общий"].values[0]
        avg_depth = top_manager["Ср. поз чек общий"].values[0] / 10

        period = now.strftime('%B %Y')

        message = (
            f"🏆 Лучший менеджер за {period}:\n\n"
            f"👤 {name}\n"
            f"📊 Выручка: {format_ruble(total)}\n"
            f"🧾 Ср. чек: {format_ruble(avg_check)}\n"
            f"📏 Глубина чека: {avg_depth:.1f}"
        )

        await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")
