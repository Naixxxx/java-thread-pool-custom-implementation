# Custom Thread Pool

Собственная реализация пула потоков для учебного задания по многопоточному программированию на Java. Пул не использует `ThreadPoolExecutor` для внутренней логики планирования задач: рабочие потоки, очереди, балансировка, политика отказа и жизненный цикл реализованы вручную.

## Что реализовано

- интерфейс `CustomExecutor extends Executor` с методами `execute`, `submit`, `shutdown`, `shutdownNow`;
- параметры `corePoolSize`, `maxPoolSize`, `keepAliveTime`, `timeUnit`, `queueSize`, `minSpareThreads`;
- несколько ограниченных очередей: у каждого `Worker` есть собственная `ArrayBlockingQueue`;
- балансировка задач между очередями по алгоритму **Least Loaded**;
- альтернативный балансировщик **Round Robin**;
- кастомная `ThreadFactory` с уникальными именами потоков и логированием создания/завершения;
- политика отказа **AbortRejectionPolicy** и альтернативная **CallerRunsRejectionPolicy**;
- `submit(Callable<T>)`, возвращающий `Future<T>`;
- корректное завершение через `shutdown()` и аварийное завершение через `shutdownNow()`;
- демонстрационная программа вместо unit-тестов;
- небольшой benchmark и tuning study для отчета.

## Требования

- Java 17+.
- Maven не обязателен: в проекте есть shell-скрипты для запуска через `javac`/`java`.
- Maven-проект также добавлен для удобства проверки в IDE.

## Запуск

### Без Maven

```bash
# демонстрация работы пула
./run-demo.sh

# сравнение с JDK ThreadPoolExecutor
./run-benchmark.sh

# мини-исследование параметров
./run-tuning.sh
```

### Через Maven

```bash
mvn compile
mvn exec:java
```

Для benchmark-классов после Maven-компиляции:

```bash
java -cp target/classes com.koshevoi.threadpool.benchmark.Benchmark
java -cp target/classes com.koshevoi.threadpool.benchmark.TuningStudy
```

## Структура проекта

```text
src/main/java/com/koshevoi/threadpool
├── CustomExecutor.java
├── CustomThreadPool.java
├── CustomThreadFactory.java
├── Worker.java
├── PoolLogger.java
├── PoolMetrics.java
├── WorkerSnapshot.java
├── balancing
│   ├── TaskBalancer.java
│   ├── LeastLoadedBalancer.java
│   └── RoundRobinBalancer.java
├── rejection
│   ├── RejectionPolicy.java
│   ├── AbortRejectionPolicy.java
│   └── CallerRunsRejectionPolicy.java
├── demo
│   ├── DemoTask.java
│   └── Main.java
└── benchmark
    ├── Benchmark.java
    └── TuningStudy.java
```

## Архитектура

```text
Client thread
    |
    | execute(Runnable) / submit(Callable)
    v
+-----------------------------+
|      CustomThreadPool       |
|-----------------------------|
| parameters                  |
| workers list                |
| TaskBalancer                |
| RejectionPolicy             |
| CustomThreadFactory         |
+--------------+--------------+
               |
               | chooses least loaded queue
               v
   +-----------+-----------+-----------+
   |                       |           |
+--v---------+       +-----v------+ +--v---------+
| Worker #1  |       | Worker #2  | | Worker #N  |
| queue #1   |       | queue #2   | | queue #N   |
| Thread #1  |       | Thread #2  | | Thread #N  |
+------------+       +------------+ +------------+
```

### Назначение основных классов

| Класс | Назначение |
|---|---|
| `CustomExecutor` | Интерфейс из задания: `execute`, `submit`, `shutdown`, `shutdownNow`. |
| `CustomThreadPool` | Главный класс пула. Создает воркеров, принимает задачи, управляет shutdown и политикой отказа. |
| `Worker` | Рабочий поток с собственной ограниченной очередью задач. |
| `CustomThreadFactory` | Создает потоки с именами вида `MainPool-worker-1` и логирует их создание/завершение. |
| `TaskBalancer` | Интерфейс стратегии распределения задач между очередями. |
| `LeastLoadedBalancer` | Выбирает очередь с минимальным количеством ожидающих задач. |
| `RoundRobinBalancer` | Альтернативный простой балансировщик «по кругу». |
| `RejectionPolicy` | Интерфейс политики отказа. |
| `AbortRejectionPolicy` | Основная политика: быстро отклоняет задачу через `RejectedExecutionException`. |
| `CallerRunsRejectionPolicy` | Альтернатива: выполняет задачу в потоке вызывающего кода. |
| `PoolLogger` | Небольшой потокобезопасный логгер без внешних зависимостей. |
| `Main` | Демонстрация нормальной работы, idle timeout, отказов и `submit`. |
| `Benchmark`, `TuningStudy` | Классы для мини-исследования производительности. |

## Параметры пула

Конструктор основной реализации:

```java
CustomThreadPool pool = new CustomThreadPool(
        "MainPool",
        2,                  // corePoolSize
        4,                  // maxPoolSize
        5,                  // keepAliveTime
        TimeUnit.SECONDS,   // timeUnit
        5,                  // queueSize на каждого worker
        1,                  // minSpareThreads
        new LeastLoadedBalancer(),
        new AbortRejectionPolicy()
);
```

| Параметр | Смысл |
|---|---|
| `corePoolSize` | Минимальное число потоков, которое пул старается поддерживать во время работы. |
| `maxPoolSize` | Верхняя граница количества потоков. |
| `keepAliveTime`, `timeUnit` | Время ожидания задачи. Если воркер простаивает дольше и потоков больше `corePoolSize`, он может завершиться. |
| `queueSize` | Емкость очереди каждого отдельного воркера. Например, при `maxPoolSize=4` и `queueSize=5` суммарно может быть до 20 задач в очередях. |
| `minSpareThreads` | Минимальное число свободных воркеров, которое пул пытается поддерживать. Если свободных воркеров становится меньше, пул создает новые потоки в пределах `maxPoolSize`. |

Проверки параметров выполняются в конструкторе. Некорректные значения приводят к `IllegalArgumentException`.

## Жизненный цикл Worker

Каждый `Worker` владеет своей `ArrayBlockingQueue<Runnable>` и работает по циклу:

1. Проверяет, не был ли вызван `shutdownNow()`.
2. Если был вызван обычный `shutdown()` и очередь пуста, завершает работу.
3. Ожидает задачу через `queue.poll(keepAliveTime, timeUnit)`.
4. Если задача получена, логирует и выполняет ее.
5. Если задача не получена за `keepAliveTime`, спрашивает пул, можно ли завершиться.
6. Воркер может завершиться по idle timeout только если после его ухода останется не меньше `corePoolSize` потоков и не будет нарушен резерв `minSpareThreads`.

При `shutdown()` уже принятые задачи дорабатываются. Новые задачи после `shutdown()` отклоняются.

При `shutdownNow()` пул:

- выставляет флаг немедленного завершения;
- очищает очереди воркеров;
- прерывает рабочие потоки;
- не запускает новые задачи из очередей.

## Балансировка задач

Основная стратегия — `LeastLoadedBalancer`.

Принцип работы:

1. Перед постановкой задачи пул делает снимок состояния воркеров: индекс, имя, размер очереди, свободная емкость, признак idle.
2. Балансировщик выбирает воркер с минимальным `queueSize`.
3. Если несколько очередей одинаково короткие, предпочтение получает idle-воркер.
4. Если выбранная очередь внезапно заполнена, пул пробует другую наименее загруженную очередь.
5. Если все очереди заполнены, но `workerCount < maxPoolSize`, создается новый воркер.
6. Если создать воркер нельзя, вызывается политика отказа.

Почему выбран Least Loaded, а не Round Robin:

- Round Robin проще и быстрее, но не учитывает реальную загрузку очередей.
- Least Loaded лучше ведет себя, когда задачи имеют разную длительность: новые задачи чаще попадают туда, где очередь короче.
- Недостаток Least Loaded — дополнительная стоимость снимка состояния воркеров при каждом `execute()`.

`RoundRobinBalancer` также реализован и может быть передан в конструктор вместо `LeastLoadedBalancer`.

## Политика отказа

По умолчанию в демонстрации используется `AbortRejectionPolicy`.

Если все очереди заполнены и количество потоков уже равно `maxPoolSize`, задача отклоняется:

```text
[Rejected] Task overload-task-5(1200ms) was rejected due to overload!
```

### Почему выбран AbortRejectionPolicy

Для серверного приложения это понятная fail-fast стратегия: если система перегружена, вызывающий код сразу получает `RejectedExecutionException` и может вернуть клиенту ошибку, применить retry, записать метрику или включить деградацию функциональности. Политика не скрывает перегрузку и не создает иллюзию, что задача будет выполнена позже.

### Недостатки

- Вызывающий код обязан обрабатывать `RejectedExecutionException`.
- При неправильной обработке можно потерять задачу.
- Для бизнес-операций, где потеря задачи недопустима, может быть лучше `CallerRunsRejectionPolicy`.

### Альтернатива: CallerRunsRejectionPolicy

`CallerRunsRejectionPolicy` выполняет задачу в потоке, который вызвал `execute()`. Это создает back-pressure: источник задач сам замедляется, потому что начинает выполнять работу. Недостаток — может вырасти latency пользовательского запроса или заблокироваться важный поток приложения.

## Логирование

Логи пишутся в `System.out` через `PoolLogger`. Формат:

```text
[HH:mm:ss.SSS] [Component] message
```

Покрытые события:

| Событие | Пример |
|---|---|
| Создание потока | `[ThreadFactory] Creating new thread: MainPool-worker-1` |
| Старт воркера | `[Worker] MainPool-worker-1 started.` |
| Прием задачи | `[Pool] Task accepted into queue of MainPool-worker-1: normal-task-1(700ms)` |
| Выполнение задачи | `[Worker] MainPool-worker-2 executes normal-task-2(700ms)` |
| Отказ | `[Rejected] Task overload-task-5(1200ms) was rejected due to overload!` |
| Idle timeout | `[Worker] IdlePool-worker-3 idle timeout, stopping.` |
| Завершение воркера | `[Worker] MainPool-worker-1 terminated.` |
| `shutdown()` | `[Pool] shutdown requested. Already queued tasks will be completed.` |
| `shutdownNow()` | `[Pool] shutdownNow requested. Dropped queued tasks: N` |

## Демонстрационная программа

Класс `com.koshevoi.threadpool.demo.Main` содержит четыре сценария.

1. **Normal work + graceful shutdown**  
   Создается пул `corePoolSize=2`, `maxPoolSize=4`, `queueSize=5`, `keepAliveTime=5 секунд`, `minSpareThreads=1`. В пул отправляются имитационные задачи, затем вызывается `shutdown()`. Все ранее принятые задачи завершаются.

2. **Idle timeout**  
   Пул увеличивается под нагрузкой до дополнительных потоков. После завершения задач лишние потоки завершаются по `keepAliveTime`.

3. **Overload and rejection**  
   Создается маленький пул `corePoolSize=1`, `maxPoolSize=2`, `queueSize=1`, затем отправляется больше задач, чем он может принять. Лишние задачи отклоняются через `AbortRejectionPolicy`.

4. **submit(Callable)**  
   Проверяется, что `submit` возвращает `Future`, из которого можно получить результат.

Фрагмент ожидаемых логов:

```text
[ThreadFactory] Creating new thread: MainPool-worker-1
[Pool] Task accepted into queue of MainPool-worker-1: normal-task-1(700ms)
[Worker] MainPool-worker-1 executes normal-task-1(700ms)
[Rejected] Task overload-task-5(1200ms) was rejected due to overload!
[Worker] IdlePool-worker-3 idle timeout, stopping.
[Worker] MainPool-worker-1 terminated.
```

## Анализ производительности

### Сравнение с JDK

Для сравнения используется `com.koshevoi.threadpool.benchmark.Benchmark`.

Условия:

- Java 21 runtime, компиляция с `--release 17`;
- 20 000 коротких CPU-bound задач;
- `corePoolSize=4`, `maxPoolSize=8`;
- для custom pool: 8 очередей максимум по 256 задач;
- для bounded JDK pool: одна `ArrayBlockingQueue` на 2048 задач;
- политика перегрузки для benchmark: `CallerRunsPolicy`, чтобы не терять задачи во время измерения;
- логирование выключено.

Результат одного запуска:

| Executor | Time, ms | Tasks/sec |
|---|---:|---:|
| `CustomThreadPool` | 483.01 | 41 407.12 |
| `JDK ThreadPoolExecutor bounded` | 58.33 | 342 869.06 |
| `JDK fixedThreadPool` | 31.93 | 626 277.65 |

Вывод: на очень коротких CPU-bound задачах стандартные реализации JDK значительно быстрее. Это ожидаемо: `ThreadPoolExecutor` сильно оптимизирован, использует одну общую очередь и не делает снимок всех воркеров при каждой постановке задачи. В текущей реализации custom pool платит за гибкость: отдельные очереди, балансировку, проверку резерва `minSpareThreads` и более подробное управление состоянием.

При более длинных задачах, например задачах с I/O или `sleep`, относительная стоимость планирования становится меньше, и главное преимущество custom pool — управляемое поведение под перегрузкой, отдельные очереди и наблюдаемые логи.

### Мини-исследование параметров

Класс `TuningStudy` проверяет несколько конфигураций custom pool на 10 000 коротких CPU-bound задачах.

Результат одного запуска:

| Name | core | max | queue | spare | balancer | Time, ms | Tasks/sec |
|---|---:|---:|---:|---:|---|---:|---:|
| small-queue | 2 | 4 | 32 | 0 | LeastLoaded | 309.21 | 32 340.62 |
| balanced | 4 | 8 | 256 | 1 | LeastLoaded | 188.27 | 53 116.00 |
| large-queue | 4 | 8 | 1024 | 1 | LeastLoaded | 149.39 | 66 937.87 |
| fixed-8 | 8 | 8 | 256 | 0 | LeastLoaded | 119.11 | 83 952.57 |
| round-robin | 4 | 8 | 256 | 1 | RoundRobin | 131.65 | 75 956.63 |

Наблюдения:

1. Слишком маленькая очередь ухудшает throughput: задачи чаще уходят в `CallerRunsRejectionPolicy`, и поток-отправитель начинает выполнять работу сам.
2. Увеличение очереди до 256–1024 задач снижает частоту отказов и улучшает пропускную способность на burst-нагрузке.
3. `core=max=8` оказался самым быстрым вариантом среди проверенных для коротких CPU-bound задач, потому что пул не тратит время на динамический рост.
4. `RoundRobinBalancer` быстрее `LeastLoadedBalancer`, потому что не ищет минимальную очередь. Но он хуже подходит для задач разной длительности.
5. `minSpareThreads=1` полезен для снижения задержки при новой волне задач, но для чистого throughput может быть лишней стоимостью, потому что заставляет пул заранее создавать дополнительные потоки.

Практический вывод:

- для коротких CPU-bound задач лучше держать фиксированное число потоков около числа доступных ядер и использовать простой балансировщик;
- для смешанных задач разной длительности лучше `LeastLoadedBalancer`;
- для burst-нагрузки нужна очередь не слишком маленького размера;
- `minSpareThreads` стоит включать, когда важнее latency, чем максимальная пропускная способность.

## Отличия от стандартного ThreadPoolExecutor

| Критерий | CustomThreadPool | ThreadPoolExecutor |
|---|---|---|
| Очереди | Несколько очередей, по одной на worker | Обычно одна общая очередь |
| Балансировка | Настраиваемая стратегия `TaskBalancer` | Встроенная логика работы с общей очередью |
| `minSpareThreads` | Есть | Нет прямого аналога |
| Логирование жизненного цикла | Встроено в реализацию | Нужно добавлять отдельно |
| Производительность на микрозадачах | Ниже из-за дополнительной логики | Обычно выше |
| Гибкость экспериментов | Выше | Ограничена стандартной моделью |

## Ограничения текущей реализации

- `queueSize` задается на каждого worker, а не глобально на весь пул.
- `awaitTermination` добавлен как удобный метод реализации, но не входит в интерфейс `CustomExecutor` из задания.
- В benchmark результаты зависят от железа, версии JVM и фоновой нагрузки.
- Для production-кода стоило бы добавить метрики через Micrometer/JMX, более строгие тесты гонок и отдельный режим graceful-drain с таймаутом.
