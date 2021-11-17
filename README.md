# Calcirix

Универсальный SQL адаптер для Clusterix-New

## Использование

Вызов адаптера:

```shell
java -jar Calcirix-jar-with-dependencies.jar "DataSourceUrl=jdbc:mysql://127.0.0.1/tpch?useCursorFetch=true&defaultFetchSize=100000;Username=root;Password=123456;Schema=s;DriverClassName=com.mysql.cj.jdbc.Driver;Query=SELECT * FROM s.nation"
```

Результат выполения будет выведен в стандартный вывод с разделетилем `;`:

```text
0;ALGERIA;0; haggle. carefully final deposits detect slyly agai
1;ARGENTINA;1;al foxes promise slyly according to the regular accounts. bold requests alon
2;BRAZIL;1;y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special 
3;CANADA;1;eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4;EGYPT;4;y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
```

Для передачи результата в файл можно воспользовтаься перенаправление вывода:

```shell
ava -jar Calcirix-jar-with-dependencies.jar "DataSourceUrl=jdbc:mysql://127.0.0.1/tpch?useCursorFetch=true&defaultFetchSize=100000;Username=root;Password=123456;Schema=s;DriverClassName=com.mysql.cj.jdbc.Driver;Query=SELECT * FROM s.nation" > resultFile.csv
```

## Строка подключения

```
DataSourceUrl=jdbc:mysql://127.0.0.1/tpch?useCursorFetch=true&defaultFetchSize=100000;Username=root;Password=123456;Schema=s;DriverClassName=com.mysql.cj.jdbc.Driver;Query=SELECT * FROM s.nation
```

Здесь:
- DataSourceUrl - указание ареса подключения
- DriverClassName - навание драйвера подключения
- Username - имя пользователя
- Password - пароль пользователя
- Schema - название главной схемы
- Query - запрос

