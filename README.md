# scraping_ez
scraping eztimize history data
Хранение данных:
1. Данные о тикерах: список тикеров, квартал (устанавливается компанией и часто не совпадает с календарным), 
   дата ближайшего аннонса и время (BMO or AMC), sector по классификации Estimize
   TODO: Должен загружаться ежедневно. Добавить поле о числе оценок, заполнять его при считывании данных EPS&Revenue
2. Данные EPS&Revenue: 
   MultiIndex: level_0 - calendar_qrt, level_1 - sources
   3 колонки на компанию - column MultiIndex: level_0 - тикер, level_1 - tic_qrt, eps, revenue т.е. будет ~ 6000 колонок
   если 404 ошибка, в колонку не пишется ничего
   Предполагается единый файл: исторические данные + ежедневно дописываем текущие
3. Отчет о загрузках:
   TODO: дата загрузки данных о тикере, дата заргузки исторических данных, дата загрузки текущих данных
