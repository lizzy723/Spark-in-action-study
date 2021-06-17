# Spark study 
참조 서적: Spark in Action (https://www.manning.com/books/spark-in-action) <br>
참조 github: https://github.com/spark-in-action/first-edition
<details close>
<summary>Original readme</summary>
  <h1>Spark in Action book repository</h1>
  <h4> Current edition: Manning Early Access Program (MEAP)</h4>

  The MEAP publishing date is **2015.04.04.**  
  Manning's book forum: [https://forums.manning.com/forums/spark-in-action](https://forums.manning.com/forums/spark-in-action)

  The repo contains book listings organized by chapter and programming language (if applicable):

  ```
  ch02
    ├── scala
    │    ├── ch02-listings.scala
    │    ├── scala-file.scala
    │    └── ...
    ├── java
    │    ├── Class1.java
    │    ├── Class2.java
    │    └── ...
    ├── python
    │    ├── ch02-listings.py
    │    ├── python-file.py
    │    └── ...
  ch03
    ├── scala
    │    ├── ch03-listings.scala
    │    ├── scala-file.scala
    │    └── ...
    ├── java
    │    ├── Class1.java
    │    ├── Class2.java
    │    └── ...
    ├── python
    │    ├── ch03-listings.py
    │    ├── python-file.py
    │    └── ...

  ```

  We tried to organize the listings so that you have minimum distractions while going through the book.  
  We hope you'll find the content useful and that you'll have fun reading the book and going through examples.  

  As part of Manning's "in action" series, it's a hands-on, tutorial-style book.  

  Thank you,  
  [Petar Zečević](http://twitter.com/p_zecevic) and [Marko Bonaći](http://twitter.com/markobonaci)
</details>


## 1장 First steps
<details close>
<summary><b>chapter 1</b> Introduction to apache spark</summary>
  <br>
  <blockquote>
  <details close>
  <summary>1.1 What is spark?</summary>
    <blockquote>
      
- **hadoop**: open source, distributed, Java computation framework consisting of the (1) **Hadoop Distributed File System(HDFS**) and (2) **MapReduce**, its execution engine. 
Hadoop solved the three main problems facing any distributed data-processing endeavor:
    - Parallelization— How to parallelize the computation (전체 연산을 잘게 나누어 동시에 처리하는 방법)
    - Distribution— How to distribute the data (데이터를 여러 노드로 분산하는 방법)
    - Fault-tolerance— How to handle component failure (분산 컴포넌트 장애에 대응하는 방법)

    → 하둡의 한계: 맵리듀스 잡의 결과를 다른 잡에서 사용하려면 먼저 이 결과를 HDFS에 저장해야 한다. 따라서 맵리듀스는 이전 잡의 결과가 다음 작업의 입력이 되는 반복 알고리즘에는 본질적으로 맞지 않다. 

- **Spark**와 hadoop의 차이: spark는 메모리에 아주 큰 데이터를 keep 하는 것을 가능하게 한다. 즉, 맵리듀스처럼 잡에 필요한 데이터를 디스크에서 매번 가져오는 대신(→ 하둡의 방법), 데이터를 메모리에 캐시로 저장하는 인-메모리 실행 모델에 있다. 이를 통해 스파크 프로그램은 맵리듀스보다 약 100배 더 빠른 속도로 같은 작업을 수행할 수 있다.
- 일부 애플리케이션은 스파크를 사용하기에 적합하지 않다. 스파크에서는 분산 아키텍처 때문에 처리 시간에 약간의 오버헤드가 필연적으로 발생한다. 대량의 데이터를 다룰 때는 오버헤드가 무시할 수 있는 수준이지만, 단일 머신에서도 충분히 처리할 수 있는 데이터셋을 다룰 때는 작은 데이터셋의 연산에 최적화된 다른 프레임워크(e.g. RDBMS)를 사용하는 것이 더 효율적이다. + 하지만 현대의 데이터 크기는 계속 증가하는 추세며, 언제가는 RDBMS와 스크립트 성능이 개선되는 속도를 추월할지도 모른다.
    </blockquote>
  </details>
    
  <details close>
  <summary>1.2 Spark components</summary>
    <blockquote><br>
스파크는 여러 특수한 목적에 맞게 설계된 다양한 컴포넌트로 구성된다.<br>
→ 스파크 core, 스파크 SQL, 스파크 Streaming, 스파크 GraphX, 스파크 MLlib <br><br>
<img width="629" alt="Screen Shot 2021-06-10 at 9 33 02 PM" src="https://user-images.githubusercontent.com/43725183/121525578-78936300-ca33-11eb-8dae-549e2a93f21a.png">

- **스파크 core**: 스파크 코어는 스파크 잡과 다른 스파크 컴포넌트에 필요한 기본 기능을 제공한다. RDD는 분산 데이터 컬렉션(즉, 데이터셋)을 추상화한 객체로 데이터셋에 적용할 수 있는 연산 및 변환 메서드를 함께 제공한다. RDD는 노드에 장애가 발생해도 데이터셋을 재구성할 수 있는 복원성을 갖추었다. broadcast variable(공유변수)와 accumulator(누적변수)를 사용해 컴퓨팅 노드 간에 정보를 공유할 수 있다. 이외에도 스파크 코어에는 네트워킹, 보안, 스케줄링 및 데이터 셔플링 등 기본 기능이 구현되어 있다.
- **스파크 SQL**
- **스파크 Streaming**
- **스파크 GraphX**
- **스파크 MLlib**
    </blockquote>
  </details>
    
  <details close>
  <summary>1.3 Spark program flow</summary>
    <blockquote><br>
예제를 통해 스파크 프로그램이 어떻게 실행되는지 알아보자.<br>
**[예제] 로그파일로부터 OutOfMemoryError 오류가 몇건 발생했는지 분석하기**

1. 노드 세 개로 구성된 하둡 클러스터에 300MB 크기의 로그 파일을 저장되어 있는 상황
→ HDFS는 자동으로 128MB크기의 chunck로 분할하고(하둡에서는 block이라는 용어를 사용한다.), 각 **블록(block)**을 클러스터의 여러 노드에 나누어 저장한다. 
→ 이 예제와 직접적인 관련은 없지만, HDFS는 노드에 저장한 각 블록을 다른 노드 두 개에 복제한다. (복제 계수가 기본 값은 3으로 설정된 경우)<br><img width="632" alt="Screen Shot 2021-06-10 at 9 55 04 PM" src="https://user-images.githubusercontent.com/43725183/121528532-85fe1c80-ca36-11eb-93e1-ba484fc4f839.png">

2. HDFS에 저장된 텍스트 파일을 메모리에 로드한다. 
→ 먼저 스파크 셸을 시작하고, 스파크 클러스터에 연결하기.
→ **데이터 지역성**(data locality, 즉 디스크에 저장된 파일이 모두 해당 노드 각각에서 RAM으로 로드되는 것/대량의 데이터를 네트워크로 전송해야하는 상황을 만들지 않기 위해!)을 최대한 달성하려고 로그 파일의 각 블록이 저장된 위치를 하둡에게 요청한 후, 모든 블록을 클러스터 노드의 RAM 메모리로 전송한다. 
→ RAM에 로딩된 블록을 **partition**이라고 한다. 이 파티션 집합이 바로 RDD가 참조하는 분산 컬렉션이다. RDD API를 사용해 RDD의 컬렉션을 필터링하고, 사용자 함수로 매핑하고, 누적 값 하나로 리듀스하고, 두 RDD를 서로 빼거나 교차하거나 결합하는 등 다양한 작업을 실행할 수 있다. 

    ```scala
    val lines = sc.textFile("hdfs://path/to/the/file")
    ```
      <img width="632" alt="Screen Shot 2021-06-10 at 9 55 21 PM" src="https://user-images.githubusercontent.com/43725183/121528576-90b8b180-ca36-11eb-8e1d-beca8097e67a.png">
      
3. 컬렉션이 OutOfMemoryError 문자열을 포함한 줄만 필터링
→ 필터링이 완료되면 RDD에는 분석에 필요한 데이터만 포함된다. 
→ 마지막으로 로그에 남은 줄 개수를 센다. 

    ```scala
    val oomLines = lines.filter(l => l.contains("OutOfMemoryError")).cache()
    val result = oomLines.count()
    ```
      <img width="634" alt="Screen Shot 2021-06-10 at 9 55 38 PM" src="https://user-images.githubusercontent.com/43725183/121528614-99a98300-ca36-11eb-85ff-f57f68c46ae6.png">
    </blockquote>
  </details>
    
  <details close>
  <summary>1.4 Spark ecosystem</summary>
    <blockquote><br>
      
      
* 하둡 생태계: 인터페이스 도구, 분석 도구, 클러스터 관리 도구, 인프라 도구로 구성되어 있다.
      
<br><img width="643" alt="Screen Shot 2021-06-10 at 9 45 04 PM" src="https://user-images.githubusercontent.com/43725183/121527201-1fc4ca00-ca35-11eb-957f-e701657ebda6.png">
- 하둡생태계의 일부 컴포넌트는 스파크 컴포넌트로 대체될 수 있다. <br>
e.g. Giraph → 스파크 GraphX, <br>
Mahout → 스파크 MLlib, <br>
Strom → 스파크 스트리밍, <br>
pig, sqoop → 스파크 코어, 스파크 SQL
- 하둡 생태계의 인프라 및 관리 도구들은 스파크로 대체할 수 없다. e.g. Oozie(→ Job scheduling), HBase, Zookeeper
- 스파크에 반드시 HDFS 스토리지를 사용할 필요는 없다. 스파크는 HDFS 외에도 **아마존 S3 버킷**이나 **일반 파일 시스템**에 저장된 데이터를 처리할 수 있다.
    </blockquote>
  </details>
    
  <details close>
  <summary>1.5 Setting up the spark-in-action VM</summary>
    <blockquote><br>

* vagrant와 virtualbox를 이용
    </blockquote>
  </details>
  </blockquote>
</details>


<details close>
<summary><b>chapter 2</b> Spark fundamentals</summary>
  <br><blockquote>
  <details close>
  <summary>2.1. Using the spark-in-action VM</summary>
    
```bash
$ vagrant up #가상 머신 시작
$ vagrant ssh -- -l spark #ssh 로그인

$ git clone https://github.com/spark-in-action/first-edition #github repository clone
```

cf. **하둡 사용하기**: `hadoop fs`로 시작한다. e.g. `hadoop fs -ls /user` (/user 폴더내 list 출력) → [http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html) 참고하기
  </details>
  <details close>
  <summary>2.2. Using Spark shell and writing your first Spark program</summary>

- license 파일을 읽어들이고, 이 파일에 포함된 BSD 문자열의 수를 세보자.

    ```python
    #license 파일을 읽기
    licLines = sc.textFile("/usr/local/spark/LICENSE")
    lineCnt = licLines.count()

    #BSD 문자열 필터링
    bsdLines = licLines.filter(lambda line: "BSD" in line)
    bsdLines.count()

    #BSD 문자열 출력
    bsdLines.foreach(lambda bLine: print(bLine))
    ```

- RDD의 개념: RDD는 스파크의 기본 추상화 객체로 다음과 같은 성질이 있다.
    - **불변성(immutable)**: 읽기 전용
    → RDD는 데이터를 조작할 수 있는 다양한 변환 연산자를 제공하지만, 변환 연산자는 항상 새로운 RDD를 생성한다.
    - **복원성(resilient)**: 장애 내성
    - **분산(distributed):** 노드 한개 이상에 저장된 데이터셋
- **RDD lineage**: 앞의 예제에서 LICENSE 파일을 로드해 `licLines` RDD를 생성했다. 그 다음 `licLines` RDD에 `filter` 함수를 적용해 새로운 `bsdLines` RDD를 생성했다. 이처럼 RDD에 적용된 변환 연산자와 그 적용 순서를 RDD lineage라고 한다.
  </details>
  <details close>
  <summary>2.3. Basic RDD actions and transformations</summary><br>
    
1. **transformation**: RDD를 조작해 새로운 RDD를 생성한다. e.g. filter, map
2. **actions**: 계산 결과를 반환하거나 RDD 요소에 특정 작업을 수행하려고 실제 계산을 시작하는 역할을 한다. e.g. count, foreach, collect, first

> 변환 연산자의 **지연 실행(lazy evaluation)**: 변환 연산자의 지연 실행은 행동 연산자를 호출하기 전까지는 변환 연산자의 계산을 실제로 실행하지 않는 것을 의미한다. RDD에 행동 연산자가 호출되면 스파크는 해당 RDD의 lineage를 살펴보고, 이를 바탕으로 실행해야 하는 연산 그래프를 작성해서 행동 연산자를 계산한다.  **결국 변환 연산자는 행동 연산자를 호출했을때 무슨 연산이 어떤 순서로 실행되어야 할지 알려주는 일종의 설계도라고 할 수 있다.** 

---

- **`map`**: RDD의 모든 요소에 임의의 함수를 적용할 수 있는 변환 연산자.

    ```python
    numbers = sc.parallelize(range(10, 51, 10))  #numbers = sc.makeRDD(range(10, 51, 10))도 가능.
    numbers.foreach(lambda x: print(x))
    numbersSquared = numbers.map(lambda num: num * num)
    numbersSquared.foreach(lambda x: print(x))
    ```

    `parallelize`는 sequential 객체를 받아 이 sequential 객체의 요소로 구성된 새로운 RDD를 만든다. 이 과정에서 sequential 객체의 요소는 여러 스파크의 executor로 **분산**된다. 
    (→ `print`결과를 보면 sequential 하지 않다. )

    ```python
    reversed = numbersSquared.map(lambda x: str(x)[::-1])
    reversed.foreach(lambda x: print(x))
    ```

- **`distinct`** and **`flatMap`**

    ```
    ##client-ids.log
    15,16,20,20
    77,80,94
    94,98,16,31
    31,15,20
    ```

    `flatMap`(주어진 모든 요소에 대해 연산을 진행하고, 반환한 배열의 중첩구조를 한 단계 제거하고 모든 요소를 단일 컬렉션으로 병합한다. )을 사용하면 각 요소별로 split을 진행한 결과를 하나의 array로 반환한다. 

    ```python
    lines = sc.textFile("Downloads/client-ids.log")

    ids = lines.flatMap(lambda x: x.split(","))
    ids.collect()   #리스트를 반환한다. 
    ids.first()

    intIds = ids.map(lambda x: int(x))
    intIds.collect()

    uniqueIds = intIds.distinct()
    uniqueIds.collect()
    finalCount  = uniqueIds.count()
    finalCount  #8
    ```

- **`sample`**, **`take`**, **`takeSample`** 연산으로 RDD 일부 요소 가져오기: `sample`은 변환연산자(transformation)이지만, `take`와 `takeSample`은 행동연산자(Actions)이다.

    ```python
    ##비복원 샘플링
    s = uniqueIds.sample(False, 0.3)  #withReplacement=False
    s.count()
    s.collect()

    ##복원 샘플링
    swr = uniqueIds.sample(True, 0.5)
    swr.count
    swr.collect()

    #확률값대신 정확한 개수로 RDD 요소 샘플링하기
    taken = uniqueIds.takeSample(False, 5)  #list 반환
    uniqueIds.take(3)
    ```

    `take`는 지정된 개수의 요소를 모을 때까지 RDD의 파티션을 하나씩 처리해 결과를 반환한다. → 결국 연산이 전혀 분산되지 않는다는 의미이다. 여러 파티션의 요소를 빠르게 가져오고 싶다면 드라이버의 메모리를 넘지 않도록 요소 개수를 적당히 줄이고 `collect` 연산자를 사용하자.
  </details>
  <details close>
  <summary>2.4. Double RDD functions</summary>
    
* Double(cf. float) 객체만 사용해서 RDD를 구성하면 implicit conversion을 통해 몇 가지 추가 함수를 사용할 수 있다.

    ```python
    intIds.mean()
    intIds.sum()

    intIds.stats()
    intIds.variance()
    intIds.stdev()

    intIds.histogram([1.0, 50.0, 100.0])  #1과 50사이에 몇개, 50과 100사이에 몇개가 있는지 반환
    intIds.histogram(3)  #3개의 경계로 나눴을때 (1) 경계값과, (2) 구간에 존재하는 sample의 수를 반환한다. 
    ```

* 근사 합계 및 평균계산: 지정된 제한시간 동안 근사합계 또는 근사 평균을 계산한다. 제한시간을 인자로 넣어준다.

    ```python
    intIds.sumApprox(1)
    intIds.meanApprox(1)
    ```
  </details></blockquote>
</details>
  
<details close>
<summary><b>chapter 3</b> Writing spark application</summary>
  <br><blockquote>
  <details close>
  <summary>3.1. Generating a new Spark project in Eclipse</summary

<br>Eclipse 대신 pycharm 사용하기
  </details>
  <details close>
  <summary>3.2. Developing the application</summary><br>
    
**[예제] 한 게임 회사의 모든 직원 명단과 각 직원이 수행한 푸시 횟수를 담은 일일 리포트를 개발해보자.**

1. 데이터셋 준비

    ```bash
    $ mkdir -p $HOME/sia/github-archive
    $ cd $HOME/sia/github-archive
    $ wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz

    $ gunzip *

    $ head -1 2015-03-01-0.json| jq '.'
    ```
    <img width="400" alt="Screen Shot 2021-06-16 at 7 32 50 PM" src="https://user-images.githubusercontent.com/43725183/122204059-a5cf8d80-ced9-11eb-9334-c0b2309e6910.png">

    > actor.login이 "treydock"인 누군가가 2015년 3월 1일 자정(created_at)에 "development "라는 저장소 브랜치를 생성한 기록이다. (깃허브 API 문서 참조: [https://developer.github.com/v3/activity/events/types/](https://developer.github.com/v3/activity/events/types/))

2. JSON 로드: `read.json()` 은 한 줄당 JSON 객체 하나가 저장된 파일을 로드한다. DataFrame 객체를 반환한다. 

    ```python
    from pyspark.sql import SQLContext
    import sys

    sqlContext = SQLContext(sc)
    ghLog = sqlContext.read.json("2015-03-01-0.json")
    ghLog.printSchema()
    ghLog.show(5)

    ghLog.count() #17786
    pushes = ghLog.filter("type = 'PushEvent'")
    pushes.count()  #8793
    ```
    <img width="1000" alt="Screen Shot 2021-06-16 at 7 35 14 PM" src="https://user-images.githubusercontent.com/43725183/122204352-fc3ccc00-ced9-11eb-8404-29866a84399e.png">

3. 데이터 집계: `count`외에도 `min`, `max`, `avg`, `sum` 등의 집계 함수를 제공한다. 

    ```python
    grouped = pushes.groupBy("actor.login").count()
    ordered = grouped.orderBy(grouped['count'], ascending=False)
    ```

4. 분석대상 제외 & 공유변수

    : `broadcast`를 이용해서 **공유변수**를 만들자. 공유변수를 설정하지 않고 이대로 예제 프로그램을 실행하면 스파크는 `employees`를 대략 200회(필터링 작업을 수행할 태스크 개수) 가까이 반복적으로 네트워크에 전송하게 될 것이다. 

    반면 공유변수는 클러스터의 각 노드에 정확히 한 번만 전송한다. 또 공유변수는 클러스터 노드의 메모리에 자동으로 캐시되므로 프로그램 실행 중 바로 접근할 수 있다. 

    ```python
    employees = [line.rstrip('\n') for line in open(sys.argv[2])]
    bcEmployees = sc.broadcast(employees)

    def isEmp(user):
    	return user in bcEmployees.value

    #filter에 사용하기 위해서 user-defined function 등록
    from pyspark.sql.types import BooleanType
    sqlContext.udf.register("SetContainsUdf", isEmp, returnType=BooleanType())
    filtered = ordered.filter("SetContainsUdf(login)")
    ```

5. 애플리케이션 실행

    ```bash
    $ python main.py \
    	../github-archive/2015-03-01-0.json \   #	../github-archive/*.json \
    	../ch03/ghEmployees.txt
    ```
    <img width="200" alt="Screen Shot 2021-06-16 at 7 37 01 PM" src="https://user-images.githubusercontent.com/43725183/122204609-46be4880-ceda-11eb-8628-b179a7dedcfa.png">

    ```python
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext
    import sys

    if __name__ == "__main__":
    	sc = SparkContext(conf=SparkConf())
    	sqlContext = SQLContext(sc)
    	ghLog = sqlContext.read.json(sys.argv[1])

    	pushes = ghLog.filter("type = 'PushEvent'")
    	grouped = pushes.groupBy("actor.login").count()
    	ordered = grouped.orderBy(grouped['count'], ascending=False)

    	# Broadcast the employees set	
    	employees = [line.rstrip('\n') for line in open(sys.argv[2])]

    	bcEmployees = sc.broadcast(employees)

    	def isEmp(user):
    		return user in bcEmployees.value

    	from pyspark.sql.types import BooleanType
    	sqlContext.udf.register("SetContainsUdf", isEmp, returnType=BooleanType())
    	filtered = ordered.filter("SetContainsUdf(login)")
    	filtered.show()

    	filtered.write.format(sys.argv[4]).save(sys.argv[3])
       #파일 형식에는 JSON, Parquet, JDBC등이 가능하다. 
       #세번째 인수는 저장 경로를 적어준다. 
    ```

  </details>
  <details close>
  <summary>3.3. Submitting the application</summary><br>

스파크 프로그램을 운영 환경에서 원활하게 실행할 수 있도록 프로그램에 JAR 파일을 추가하는 방법은 (1) `spark-submit` 스크립트의 `—jars` 매개변수에 프로그램에 필요한 JAR 파일을 모두 나열해 실행자로 전송하거나, (2) 모든 의존 라이브러리를 포함하는 `underjar`를 빌드하는 방법이다. 

- uderjar 빌드: 일단은 생략
- `spark-submit`: 스파크 애플리케이션을 제출하는 일종의 헬퍼 스크립트로 애플리케이션을 스파크 클러스터에서 실행하는데 사용한다.

    ```bash
    ./bin/spark-submit \
      --class <main-class> \   #python에서는 사용 x
      --master <master-url> \
      --deploy-mode <deploy-mode> \
      --conf <key>=<value> \
      ... # other options
      <application-jar> \
      [application-arguments]
    ```

    결과 폴더에 있는 `SUCCESS` 파일은 작업을 성공적으로 완료했음을 의미한다. 각 `crc` 파일은 CRC(Cyclic Redundancy Check)코드를 계산해 각 데이터 파일의 유효성을 검사하는데 사용한다. `._SUCCESS.crc` 파일은 모든 CRC파일을 검사한 결과가 성공적이라는 것을 의미한다.  

    ```bash
    $ spark-submit \
    	main.py \
    	"../github-archive/2015-03-01-0.json" \ 
    	"../ch03/ghEmployees.txt" \
    	"emp-gh-push-output" "json"
    ```
        
    <img width="1000" alt="Screen Shot 2021-06-17 at 12 08 30 PM" src="https://user-images.githubusercontent.com/43725183/122324988-bc6ef680-cf64-11eb-946b-4ede59f690a4.png">

  </details></blockquote>
</details>
  
<details close>
<summary><b>chapter 4</b> The spark api in depth</summary>
  <br><blockquote>
  <details close>
  <summary>4.1. Working with pair RDDs</summary>
  </details>
  <details close>
  <summary>4.2. Understanding data partitioning and reducing data shuffling</summary>
  </details>
  <details close>
  <summary>4.3. Joining, sorting, and grouping data</summary>
  </details>
  <details close>
  <summary>4.4. Understanding RDD dependencies</summary>
  </details></blockquote>
</details>

## 2장 meet the spark family

## 3장 spark ops

## 4장 Bringing it together

### Appendix
<details close>
<summary><b>A. Installing Apache Spark</b></summary>
  
1. 자바(JDK) 설치
    - 자바가 설치되어 있는지 확인하기: `which javac`
    (이 명령은 javac 명령을 실행했을 때 실제로 호출될 실행 파일 위치를 반환한다.)
    - (만약 자바가 설치되어 있다면)JAVA_HOME 환경 변수가 올바르게 설정되어 있는지 확인해본다.

        ```bash
        #환경 변수 설정되어 있는지 확인
        $echo $JAVA_HOME #open-jdk 설치 폴더

        #환경 변수 설정
        $echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" | sudo tee /etc/profile.d/sia.sh
        $source /etc/profile.d/sia.sh 
        ```

    - (만약 자바가 설치되어 있지 않다면)JDK 설치하기:  `sudo apt-get -y install openjdk-8-jdk`
2. 아파치 스파크 내려받고 설치 및 설정
    - 스파크 내려받기 페이지([http://spark.apache.org/downloads.html](http://spark.apache.org/downloads.html))에서 <br>
    (1) 최신 스파크 릴리스 선택(Choose a Spark release),<br>
    (2) 최신 하둡 버전으로 빌드된 패키지 유형 선택(Choose a package type),<br>
    (3) 스파크 다운로드(Download Spark)<br>
    아파치 미러의 목록이 나오면 가장 상위에 추천된 미러 사이트를 클릭한다.
    - 압축 풀고 설치: 리눅스의 표준 규칙에 따라 사용자 홈 디렉토리 아래의 bin 디렉토리를 스파크 바이너리 위치로 사용한다. 홈 디렉토리에는 전체 권한(읽기, 쓰기, 실행)이 부여되므로 매번 sudo를 입력할 필요가 없다.

        ```bash
        $cd $HOME/Downloads
        $tar -xvf spark*
        $rm spark*tgz

        ##홈디렉토리 아래의 bin 디렉토리로 스파크 바이너리 옮기기
        $cd $HOME
        $mkdir -p ~/bin/sparks
        $mv Downloads/spark-* bin/sparks
        ```

    - 여러 버전의 스파크를 사용할 경우 bin 디렉토리 아래에 sparks 디렉토리를 생성하고 이 안에 스파크의 현재 및 미래 버전을 모두 저장하자. 그리고 현재 사용할 버전에 심볼릭 링크를 만들자.

        ```bash
        $cd $HOME/bin
        $ln -s sparks/spark-2.0.0-bin-hadoop2.7 spark
        $tree -L 2 #심볼릭 링크 확인. 여기서 spark 폴더는 sparks 폴더 내의 다른 폴더를 가리키는 심볼릭 링크를 가리킨다. 

        #심볼릭 링크 바꾸기(기존의 심볼릭 링크 지우고, 새로 심볼릭 링크 만들기)
        $rm spark
        $ln -s spark-1.6.1-bin-hadoop2.6 spark
        ```

3. 스파크 셸
    - 스파크 셸 시작하기

        ```bash
        $cd $HOME/bin/spark
        $./bin/spark-shell 
        ```

    - 로깅레벨 변경하기: 스파크의 LOG4J 설정을 변경해 스파크 셸에는 오류 로그만 출력하고, 나머지 로그는 추후 문제 진단에 사용할 수 있도록 스파크 루트 폴더 아래의 logs/info.log 파일에 저장해보자.

        ```bash
        $gedit conf/log4j.properties  #스파크의 conf 디렉터리 아래에 log4j.properites 파일을 생성. log4j.properties 파일은 ch2 참조
        ```
    - 스파크 REPL(=스파크 셸)은 스파크 컨텍스트와 SparkSession 객체를 각각 sc와 spark라는 변수로 제공한다. 스파크 애플리케이션은 SparkSession을 통해 스파크에 접속하고 세션 설정, 작업 실행 관리, 파일 읽기 및 쓰기 작업 등을 수행할 수 있다. 스파크 REPL에 spark.까지만 입력하고 tab을 누르면 SparkSession이 제공하는 모든 함수를 볼 수 있다.
    - 파이썬 셸로 spark를 시작하는 경우: `$pyspark`
</details>
<details close>
  <summary><b>B. Understanding MapReduce</b></summary>
(MapReduce: Simplified Data Processing on Large Clusters)

- Job을 잘게 분할하고 클러스터의 모든 노드로 **map**ping, 즉 파견해 분산 처리를 수행하는 새로운 패러다임. 각 노드는 분할된 job을 처리한 중간 결과를 생성한다. 그 다음 분할된 중간 결과를 **reduce**, 즉 집계해 최종 결과를 낸다.
    - 맵리듀스는 최소한의 API만 노출해 대규모 분산 시스템을 다루는 복잡한 작업을 효과적으로 감추었다. map과 reduce만 노출함.
    - 맵리듀스는 데이터를 굳이 옮겨서 처리할 필요가 없다. 대신 데이터가 저장된 곳으로 "프로그램"을 전송한다.
- **Word count 문제**(빅데이터 분야의 hello world같은 존재)
한 웹 사이트에서 흔히 사용하는 단어를 분석해달라.

    ```
    Is it easy to program with MapReduce?
    Is it as easy as Map and Reduce are.
    ```

    1. `map`: 각 문장을 읽고 단어로 분할한다. 그다음 대문자를 소문자로 변환하고, 구두점을 제거한 후 각 단어마다 1이라는 값을 생성한다. 즉, key가 단어이고 value가 count(=1)이다. 
    → map은 입력된 각 문장별로 키와 값을 받아 키-값 쌍의 목록을 반환한다. 
    2. **shuffle phase:** 동일한 단어를 동일한 리듀서에 전달한다. 거의 모든 애플리케이션에서 가장 좁은 병목으로 작용한다. 
    3. `reduce`: map 함수의 return값을 받고, 단어별 출현 횟수를 합산한 후 최종 결과를 생성한다. 
- (정리) map 함수는 키-값 쌍을 가져와 임의의 변환 연산을 적용하고 중간 결과물로 키-값 쌍의 목록을 반환한다. 그런 다음 맵리듀스 API는 map 함수가 반환한 키-값 쌍을 키별로 그루핑해 reduce 함수의 입력으로 전달한다. reduce 함수는 입력된 값들을 임의의 방법으로 병합하고 그 결과를 최종 출력파일에 쓴다. 모든 리듀서가 작업을 완료하면 사용자 프로그램으로 제어를 반환한다.
</details>
<details close>
  <summary><b>C. A primer on linear algebra</b></summary><br>
  
- 행렬과 벡터
- 행렬 덧셈
- 스칼라배
- 행렬곱셈
- 단위행렬
- 역행렬: 역행렬을 갖는 정사각 행렬을 non-singular matrix라고 한다.
</details>
