#!/bin/bash

# Конфигурационные параметры
API_URL="https://xmlstock.com/yandex/xml/"
USER_ID="12220"
API_KEY="79193c9fe911b30a0f3627a5fe12902d"
GROUP_BY="100"
DOMAIN="ru"
LR="213"
DEVICE="desktop"

# Основной домен
MAIN_DOMAIN="dn.ru"

# Конкуренты (до 5 конкурентов)
# моикотлы.рф котел-диво.рф 
COMPETITORS=("ozon.ru" "vseinstrumenti.ru" "avito.ru")

# Файлы для результатов и логов
RESULT_FILE="results_main_domain.txt"
LOG_FILE="log.txt"

# Очистка файлов перед началом
> $RESULT_FILE
> $LOG_FILE
for competitor in "${COMPETITORS[@]}"; do
    > "results_${competitor}.txt"
done

# Запись даты и сервиса съема в файл результатов
echo "Дата съема: $(date)" >> $RESULT_FILE
echo "Сервис съема: Yandex XML" >> $RESULT_FILE
echo -e "URL\tЗапрос\tПозиция" >> $RESULT_FILE

for competitor in "${COMPETITORS[@]}"; do
    echo "Дата съема: $(date)" >> "results_${competitor}.txt"
    echo "Сервис съема: Yandex XML" >> "results_${competitor}.txt"
    echo -e "URL\tЗапрос\tПозиция" >> "results_${competitor}.txt"
done

# Функция для URL-кодирования строки
urlencode() {
    local data
    if [ "$#" -eq 1 ]; then
        data=$(echo -n "$1" | od -An -tx1 -w1 -v | tr ' ' % | tr -d '\n')
    else
        data=$(cat - | od -An -tx1 -w1 -v | tr ' ' % | tr -d '\n')
    fi
    echo $data
}

# Чтение запросов из файла
while IFS= read -r query; do
    echo "Processing query: $query" | tee -a $LOG_FILE

    # URL-кодирование запроса
    encoded_query=$(urlencode "$query")
    echo "Encoded query: $encoded_query" | tee -a $LOG_FILE

    # Формирование полного URL для API-запроса
    request_url="${API_URL}?user=${USER_ID}&key=${API_KEY}&query=${encoded_query}&groupby=${GROUP_BY}&domain=${DOMAIN}&lr=${LR}&device=${DEVICE}"
    echo "Request URL: $request_url" | tee -a $LOG_FILE

    # Получение XML-ответа от API
    response=$(curl -s "$request_url")
    echo "API response received." | tee -a $LOG_FILE

    # Сохранение ответа в файл для дальнейшего анализа
    echo "$response" > response.xml

    # Парсинг XML для поиска сайтов
    # Используем xmllint для парсинга XML и получения нужных данных
    group_count=$(xmllint --xpath "count(//group)" response.xml)

    for ((i=1; i<=group_count; i++)); do
        domain=$(xmllint --xpath "string(//group[$i]/doc/domain)" response.xml)
        url=$(xmllint --xpath "string(//group[$i]/doc/url)" response.xml)
        if [ -z "$url" ]; then
            url="URL не найден"
        fi
        
        # Проверка основного домена
        if [[ "$domain" == "$MAIN_DOMAIN" ]]; then
            echo -e "$url\t$query\t$i" >> $RESULT_FILE
        fi
        
        # Проверка конкурентов
        for competitor in "${COMPETITORS[@]}"; do
            if [[ "$domain" == "$competitor" ]]; then
                echo -e "$url\t$query\t$i" >> "results_${competitor}.txt"
            fi
        done
    done

    # Удаление временного файла
    rm response.xml

    # Пауза между запросами, чтобы избежать ограничения по частоте запросов
    sleep 1
done < queries.txt

echo "Processing completed. Results are saved in $RESULT_FILE and respective competitor files." | tee -a $LOG_FILE
