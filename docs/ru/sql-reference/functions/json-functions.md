# Функции для работы с JSON {#funktsii-dlia-raboty-s-json}

В Яндекс.Метрике пользователями передаётся JSON в качестве параметров визитов. Для работы с таким JSON-ом, реализованы некоторые функции. (Хотя в большинстве случаев, JSON-ы дополнительно обрабатываются заранее, и полученные значения кладутся в отдельные столбцы в уже обработанном виде.) Все эти функции исходят из сильных допущений о том, каким может быть JSON, и при этом стараются почти ничего не делать.

Делаются следующие допущения:

1.  Имя поля (аргумент функции) должно быть константой;
2.  Считается, что имя поля в JSON-е закодировано некоторым каноническим образом. Например, `visitParamHas('{"abc":"def"}', 'abc') = 1`, но `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  Поля ищутся на любом уровне вложенности, без разбора. Если есть несколько подходящих полей - берётся первое.
4.  В JSON-е нет пробельных символов вне строковых литералов.

## visitParamHas(params, name) {#visitparamhasparams-name}

Проверить наличие поля с именем name.

## visitParamExtractUInt(params, name) {#visitparamextractuintparams-name}

Распарсить UInt64 из значения поля с именем name. Если поле строковое - попытаться распарсить число из начала строки. Если такого поля нет, или если оно есть, но содержит не число, то вернуть 0.

## visitParamExtractInt(params, name) {#visitparamextractintparams-name}

Аналогично для Int64.

## visitParamExtractFloat(params, name) {#visitparamextractfloatparams-name}

Аналогично для Float64.

## visitParamExtractBool(params, name) {#visitparamextractboolparams-name}

Распарсить значение true/false. Результат - UInt8.

## visitParamExtractRaw(params, name) {#visitparamextractrawparams-name}

Вернуть значение поля, включая разделители.

Примеры:

``` sql
visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'
```

## visitParamExtractString(params, name) {#visitparamextractstringparams-name}

Распарсить строку в двойных кавычках. У значения убирается экранирование. Если убрать экранированные символы не удалось, то возвращается пустая строка.

Примеры:

``` sql
visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
visitParamExtractString('{"abc":"hello}', 'abc') = ''
```

На данный момент, не поддерживаются записанные в формате `\uXXXX\uYYYY` кодовые точки не из basic multilingual plane (они переводятся не в UTF-8, а в CESU-8).

Следующие функции используют [simdjson](https://github.com/lemire/simdjson) который разработан под более сложные требования для разбора JSON. Упомянутое выше предположение 2 по-прежнему применимо.

## isValidJSON(json) {#isvalidjsonjson}

Проверяет, является ли переданная строка валидным json значением.

Примеры:

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

## JSONHas(json\[, indices\_or\_keys\]…) {#jsonhasjson-indices-or-keys}

Если значение существует в документе JSON, то возвращается `1`.

Если значение не существует, то возвращается `0`.

Примеры:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

`indices_or_keys` — это список из нуля или более аргументов каждый из них может быть либо строкой либо целым числом.

-   Строка — это доступ к объекту по ключу.
-   Положительное целое число — это доступ к n-му члену/ключу с начала.
-   Отрицательное целое число — это доступ к n-му члену/ключу с конца.

Адресация элементов по индексу начинается с 1, следовательно элемент 0 не существует.

Вы можете использовать целые числа, чтобы адресовать как массивы JSON, так и JSON-объекты.

Примеры:

``` sql
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'
```

## JSONLength(json\[, indices\_or\_keys\]…) {#jsonlengthjson-indices-or-keys}

Возвращает длину массива JSON или объекта JSON.

Если значение не существует или имеет неверный тип, то возвращается `0`.

Примеры:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## JSONType(json\[, indices\_or\_keys\]…) {#jsontypejson-indices-or-keys}

Возвращает тип значения JSON.

Если значение не существует, то возвращается `Null`.

Примеры:

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

## JSONExtractUInt(json\[, indices\_or\_keys\]…) {#jsonextractuintjson-indices-or-keys}

## JSONExtractInt(json\[, indices\_or\_keys\]…) {#jsonextractintjson-indices-or-keys}

## JSONExtractFloat(json\[, indices\_or\_keys\]…) {#jsonextractfloatjson-indices-or-keys}

## JSONExtractBool(json\[, indices\_or\_keys\]…) {#jsonextractbooljson-indices-or-keys}

Парсит JSON и извлекает значение. Эти функции аналогичны функциям `visitParam`.

Если значение не существует или имеет неверный тип, то возвращается `0`.

Примеры:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## JSONExtractString(json\[, indices\_or\_keys\]…) {#jsonextractstringjson-indices-or-keys}

Парсит JSON и извлекает строку. Эта функция аналогична функции `visitParamExtractString`.

Если значение не существует или имеет неверный тип, то возвращается пустая строка.

У значения убирается экранирование. Если убрать экранированные символы не удалось, то возвращается пустая строка.

Примеры:

``` sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
SELECT JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
SELECT JSONExtractString('{"abc":"hello}', 'abc') = ''
```

## JSONExtract(json\[, indices\_or\_keys…\], Return\_type) {#jsonextractjson-indices-or-keys-return-type}

Парсит JSON и извлекает значение с заданным типом данных.

Это обобщение предыдущих функций `JSONExtract<type>`.
Это означает
`JSONExtract(..., 'String')` выдает такой же результат, как `JSONExtractString()`,
`JSONExtract(..., 'Float64')` выдает такой же результат, как `JSONExtractFloat()`.

Примеры:

``` sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))') = ('hello',[-100,200,300])
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)') = ([-100,200,300],'hello')
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))') = [-100, NULL, NULL]
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)') = NULL
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8') = 1
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Thursday'
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Friday'
```

## JSONExtractKeysAndValues(json\[, indices\_or\_keys…\], Value\_type) {#jsonextractkeysandvaluesjson-indices-or-keys-value-type}

Разбор пар ключ-значение из JSON, где значение имеет тип данных ClickHouse.

Пример:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)];
```

## JSONExtractRaw(json\[, indices\_or\_keys\]…) {#jsonextractrawjson-indices-or-keys}

Возвращает часть JSON в виде строки, содержащей неразобранную подстроку.

Если значение не существует, то возвращается пустая строка.

Пример:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]'
```

## JSONExtractArrayRaw(json\[, indices\_or\_keys\]…) {#jsonextractarrayrawjson-indices-or-keys}

Возвращает массив из элементов JSON массива, каждый из которых представлен в виде строки с неразобранными подстроками из JSON.

Если значение не существует или не является массивом, то возвращается пустой массив.

Пример:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"']'
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/json_functions/) <!--hide-->
