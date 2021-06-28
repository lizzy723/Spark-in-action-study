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
  <summary>4.1. Working with pair RDDs</summary><br>
    
키-값 쌍은 전통적으로 연관배열(associative array)이라는 자료구조를 사용해 표현한다. 이 자료구조를 파이썬에서는 dictionary라고 하며, 스칼라와 자바에서는 map이라고 한다. 스파크에서는 키-값 쌍으로 구성된 RDD를 **pair RDD**라고 한다. 스파크의 데이터를 반드시 키-값 쌍 형태로 구성할 필요는 없지만, 실무에서는 Pair RDD를 사용하는 것이 바람직한 경우가 많다. 

- **Pair RDD 생성**: RDD의 keyBy 변환 연산자는 RDD 요소로 키를 생성하는 f 함수를 받고, 각 요소를 (f(요소), 요소)( = **(키, 요소)**) 쌍의 튜플로 매핑한다. 물론 데이터를 2-element 튜플로 직접 변환할 수도 있다. 어떤 방법이든 2-element 튜플로 RDD를 구성하면 **Pair RDD 함수**가 RDD에 자동으로 추가된다.

    ```python
    tranFile = sc.textFile("ch04/ch04_data_transactions.txt")
    tranData = tranFile.map(lambda line: line.split("#"))
    #예제 파일의 각 줄에는 구매 날짜, 시간, 고객 ID, 상품 ID, 구매수량, 구매 금액이 '#'로 구분되어있다. 
    ```
    <img width="400" alt="Screen Shot 2021-06-18 at 4 42 12 PM" src="https://user-images.githubusercontent.com/43725183/122525386-24503a80-d054-11eb-996d-d9a75f962093.png">

    ```python
    #키, 요소 페어 생성
    transByCust = tranData.map(lambda t: (int(t[2]), t))

    transByCust.keys().distinct().count() #100
    transByCust.count() #1000
    #map 함수로 키와 값을 만들어보자. 고객이 총 100명 있다.
    ```

- **Pair RDD 함수**
    - 키별 개수 세기(`CountByKey`): 각 키의 출현 횟수를 담은 Map 객체를 반환한다. ID가 53인 고객이 19번으로 가장 많이 구매했다.

        ```python
        transByCust.countByKey()
        cid, purch = sorted(transByCust.countByKey().items(), key= lambda x:-x[1])[0] ##(53, 19)

        #사은품 리스트 추가(가격 = 0)
        complTrans = [["2015-03-30", "11:59 PM", "53", "4", "1", "0.00"]]
        ```

    - 단일 키로 값 찾기: `transByCust.lookup(53)` → 53번 고객의 모든 구매 기록을 가져올 수 있다. lookup은 결과 값을 드라이버로 전송하므로 이를 메모리에 적재할 수 있는지 먼저 확인해야 한다.
        
        <img width="300" alt="Screen Shot 2021-06-18 at 4 43 00 PM" src="https://user-images.githubusercontent.com/43725183/122525491-3f22af00-d054-11eb-898a-df7f06895a28.png">

 

    - `mapValues` 변환 연산자로 Pair RDD 값 바꾸기: 키를 그대로 두고 값만 변경할 수 있다. 25번 상품을 2번 이상 구매한 경우 5% 할인해주자.

        ```python
        def applyDiscount(tran):
            if(int(tran[3])==25 and float(tran[4])>1):
                tran[5] = str(float(tran[5])*0.95)
            return tran

        transByCust = transByCust.mapValues(lambda t: applyDiscount(t))
        ```

    - `flapMapValues` 변환 연산자로 키에 값 추가: 81번 다섯 개 이상 구매한 고객에게 사은품으로 70번을 보내야한다. 즉, 해당 고객이 70번 상품을 구매한 것 처럼 `transByCust`에 추가해야한다.

        ```python
        def addToothbrush(tran):
            if(int(tran[3]) == 81 and int(tran[4])>4):
                from copy import copy
                cloned = copy(tran)
                cloned[5] = "0.00"
                cloned[3] = "70"
                cloned[4] = "1"
                return [tran, cloned]
            else:
                return [tran]

        transByCust = transByCust.flatMapValues(lambda t: addToothbrush(t))
        transByCust.count()  #1006 -> 6명이 81번 상품을 5개 이상 구매했다.
        ```

    - `reduceByKey` 또는 `foldByKey` 변환 연산자로 키의 모든 값 병합하기: reduceByKey는 각 키의 모든 값을 동일한 타입의 단일 값으로 변환한다. 이 연산자에는 두 값을 하나로 병합하는 Merge 함수를 전달해야하며, reduceByKey는 각 키별로 값 하나만 남을 때까지 merge 함수를 계속 호출한다. 따라서 merge 함수는 결합법칙을 만족해야한다.

        ```python
        amounts = transByCust.mapValues(lambda t: float(t[5])) #각 키별로 값만 뽑기 
        totals = amounts.foldByKey(0, lambda p1, p2: p1 + p2).collect()
        sorted(totals, key = lambda x:x[1])[-1] #(76, 100049.0)

        #사은품 리스트 추가(가격 = 0)
        complTrans += [["2015-03-30", "11:59 PM", "76", "63", "1", "0.00"]]
        #76번 고객이 10만 49달러로 가장 많이 썼다. 
        ```

        foldByKey는 reduceByKey와 기능은 같지만, merge 함수의 인자 목록 바로 앞에 zeroValue 인자를 담은 또 다른 인자 목록을 추가로 전달해야 한다는 점은 다르다. zeroValue는 반드시 항등원이어야한다. (e.g. 덧셈에서는 0, 곱셈에서는 1)

    - `union`으로 array 추가하기 + 결과 파일 저장하기

        ```python
        transByCust = transByCust.union(sc.parallelize(complTrans).map(lambda t: (int(t[2]), t)))
        transByCust.map(lambda t: "#".join(t[1])).saveAsTextFile("ch04/ch04output-transByCust")
        ```

    - `aggregateByKey`로 키의 모든 값 그루핑: aggregateByKey는 (1) **zeroValue**와 (2) 임의의 V 타입을 가진 값을 또 다른 U 타입으로 변환하는 **변환 함수(Transform)**, (3)변환함수가 변환한 값을 두 개씩 하나로 병합하는 **병합함수(Merge)**가 필요하다.

        ```python
        prods = transByCust.aggregateByKey([], 
        		lambda prods, tran: prods + [tran[3]],    #변환함수 -> 각 파티션 별로 요소를 병합
            lambda prods1, prods2: prods1 + prods2)   #병합함수 -> 최종 결과를 병합
        prods.collect()
        #prods에 빈 리스트가 전달되었다.
        ```
        <img width="1000" alt="Screen Shot 2021-06-18 at 4 43 31 PM" src="https://user-images.githubusercontent.com/43725183/122525565-52357f00-d054-11eb-8eb7-b7f6cba80cd0.png">

  </details>
  <details close>
  <summary>4.2. Understanding data partitioning and reducing data shuffling</summary>
    
* **데이터 파티셔닝(data partitioning)** : 데이터를 여러 클러스터 노드로 분할하는 메커니즘을 의미한다. 이 장에서는 일단 스파크 클러스터를 '병렬 연산이 가능하고 네트워크로 연결된 머신(즉, 노드)의 집합'정도로 생각하자.
    - **파티션(partition)**: 과거에는 파티션 대신 스플릿(split)용어를 사용했다. RDD의 파티션은 RDD 데이터의 일부(조각 또는 슬라이스)를 의미한다. 예를 들어 로컬 파일 시스템에 저장된 텍스트 파일을 스파크에 로드하면, 스파크는 파일 내용을 여러 파티션으로 분할해 클러스터 노드에 고르게 분산 저장한다. 여러 파티션을 노드 하나에 저장할 수도 있다. 이렇게 분산된 파티션이 모여서 RDD 하나를 형성한다.
    - 파티션의 개수: 해당 RDD에 변환 연산을 실행할 "태스크 개수"와 직결되므로 파티션의 개수는 매우 중요하다. 태스크 개수가 필요 이하로 적으면 클러스터를 충분히 활용할 수 없다. 게다가 각 태스크가 처리할 데이터 분량이 실행자의 메모리 리소스를 초과해 메모리 문제가 발생할 수 있다. 따라서 클러스터의 코어 개수보다 서너 배 더 많은 파티션을 사용하는 것이 좋다.
    - **데이터 partitioner**: RDD의 데이터 파티셔닝은 RDD의 각 요소에 파티션 번호를 할당하는 partitioner 객체가 수행한다. partitioner는 HashPartitioner, RangePartitioner, 또는 사용자 정의 Partitioner(Pair RDD의 경우)로 구현할 수 있다.
        - **HashPartitioner**: 스파크의 기본 Partitioner. HashPartitioner는 각 요소의 자바 해시코드를 단순한 mod 공식(partitionIndex = hashCode % numberOfPartitions)에 대입해 파티션 번호를 계산한다. 각 요소의 파티션 번호를 거의 무작위로 결정하기 때문에 모든 파티션을 정확하게 같은 크기로 분할할 가능성이 낮다. 하지만 대규모 데이터셋을 상대적으로 적은 수의 파티션으로 나누면 대체로 데이터를 고르게 분산시킬 수 있다. 
        (파티션의 기본 개수는 spark.default.parallelism 환경 매개변수 값으로 결정된다)
        - **RangePartitioner**: 정렬된 RDD의 데이터를 거의 같은 범위 간격으로 분할할 수 있다.
        - **Pair RDD의 사용자 정의 Partitioner**: 파티션의 데이터를 특정 기준에 따라 정확하게 배치해야 할 경우 사용자 정의 partitioner로 pair RDD를 분할할 수 있다. 
        (cf. in Python there is no version of aggregateByKey with a custom partitioner)
    - RDD 파티션 변경
        - `partitionBy`: PairRDD에서만 사용가능하고, 또 파티셔닝에 사용할 Partitioner 객체만 인자로 전달할 수 있다
        - `coalesce`와 `repartition`: coalesce는 파티션의 수를 줄이거나 늘리는데 사용한다. 파티션 개수를 늘리려면 shuffle 인자를 true로 설정해야한다. 반면 파티션 수를 줄일 때는 이 인자를 false로 설정할 수 있다. 이때는 새로운 파티션 개수와 동일한 개수의 부모 RDD 파티션을 선정하고 나머지 파티션의 요소를 나누어 선정한 파티션과 병합(coalesce)하는 방식으로 파티션 개수를 줄인다. 즉, 셔플링을 수행하지 않는 대신 데이터 이동을 최소화하려고 부모 RDD의 기존 파티션을 최대한 보존한다. 
        cf. repartition 변환 연산자는 단순히 shuffle을 true로 설정해 coalesce를 호출한 결과를 반환한다.
        - `repartitionAndSortWithinPartition`: 정렬 가능한 RDD에서만 사용할 수 있다. 새로운 Partitioner 객체를 받아 각 파티션 내에서 요소를 정렬한다. 이 연산자는 셔플링 단계에서 정렬 작업을 함께 수행하기 때문에 repartition을 호출한 후 직접 정렬하는 것보다 성능이 더 낫다.
- **데이터 셔플링(data shuffling)**: **파티션 간의 물리적인 데이터 이동을 의미한다.** 셔플링은 새로운 RDD의 파티션을 만들려고 여러 파티션의 데이터를 합칠 때 발생한다. 예를 들어 키를 기준으로 요소를 그루핑하려면 스파크는 RDD의 파티션을 모두 살펴보고 키가 같은 요소를 전부 찾은 후, 이를 물리적으로 묶어서 새로운 파티션을 구성하는 과정을 수행해야한다.

    ```python
    prods = transByCust.aggregateByKey([], 
    		lambda prods, tran: prods + [tran[3]],    #변환함수(Transform) -> 각 파티션 별로 요소를 병합
        lambda prods1, prods2: prods1 + prods2)   #병합함수(Merge) -> 최종 결과를 병합
    prods.collect()
    #4.1절의 aggregateByKey 예시를 다시 보자. 
    ```
    <img width="600" alt="Screen Shot 2021-06-18 at 4 45 57 PM" src="https://user-images.githubusercontent.com/43725183/122525886-a93b5400-d054-11eb-9dcd-ea556f230a4d.png">


    - 변환함수(Transform)은 각 파티션 별로 각 키의 값을 모아서 리스트를 구성한다.(**map task**) →  스파크는 이 리스트들을 각 노드의 중간 파일(interm files)에 기록한다 → 병합함수(merge)를 호출해 여러 파티션에 저장된 리스트들을 각 키별 단일 리스트로 병합한다.(**reduce task**) → 기본 partitioner(hash partitioner)를 적용해 각 키를 적절한 파티션에 할당한다.
    - 셔플링 바로 전에 수행한 태스크를 **맵(map) 태스크**라고 하며, 바로 다음에 수행한 태스크를 **리듀스(reduce)** 태스크라고 한다. 맵 태스크의 결과는 중간 파일에 기록하며(주로 운영체제의 파일 시스템 캐시에만 저장), 이후 리듀스 태스크가 이 파일을 읽어들인다. 중간 파일을 디스크에 기록하는 작업도 부담이지만, 결국 셔플링할 데이터를 네트워크로 전송해야 하기 때문에 스파크 잡의 셔플링 횟수를 최소한으로 줄이도록 노력해야 한다.
    - 셔플링 발생 조건
        1. partitioner를 명시적으로 변경하는 경우
            - 파티션 개수가 다른 HashPatitioner를 변환 연산자에 사용하거나, (`rdd.aggregateByKey(zeroValue, seqFunc, comboFunc, 100).collect()`)
            - 사용자 정의 Partitioner를 사용하면

            → 셔플링이 발생한다. 따라서 가급적이면 기본 partitioner를 사용해 의도하지 않은 셔플링은 최대한 피하는 것이 성능 면에서 가장 안전한 방법이다. 

        2. partitioner를 제거하는 경우

            변환 연산자에 partitioner를 명시적으로 지정하지 않았는데도 간혹 셔플링이 발생할 때가 있다. 대표적으로 map과 flatMap은 RDD의 Partitioner를 제거한다. 이 연산자 자체로는 셔플링이 발생하지 않지만, 연산자의 결과 RDD에 다른 변환 연산자를 사용하면 기본 Partitioner를 사용했더라도 여전히 셔플링이 발생한다.  → Pair RDD의 키를 변경하지 않는다면 map, flatMap 대신 mapValues, flatMapValues를 사용해 Partitioner를 보존하는 것이 좋다. 

            ```python
            rdd = sc.parallelize(range(10000))
            rdd.map(lambda x: (x, x*x)).map(lambda (x, y): (y, x)).collect() #셔플링 발생 x
            rdd.map(lambda x: (x, x*x)).reduceByKey(lambda v1, v2: v1+v2).collect() #셔플링 발생함
            ```

    - 셔플링을 수행하면 executor는 다른 executor(=실행자)의 파일을 읽어 들여야한다. 하지만 셔플링 도중 일부 실행자에 장애가 발생하면 해당 실행자가 처리한 데이터를 더 이상 가져올 수 없어서 데이터 흐름이 중단된다. → **외부 셔플링 서비스(external shuffling service)**는 실행자가 중간 셔플 파일을 읽을 수 있는 단일 지점을 제공해 셔플링의 데이터 교환 과정을 최적화할 수 있다. (`spark.shuffle.service.enabled=true`로 설정하기)
    - 셔플링 관련 매개변수
        - 셔플링 알고리즘 설정: `spark.shuffle.manager`의 값을 hash(해시 기반 셔플링), sort(정렬 기반 셔플링) → 정렬 기반 셔플링은 파일을 더 적게 생성하고 메모리를 더욱 효율적으로 사용할 수 있어 default 값이다.
        - 중간 파일의 통합 여부: `spark.shuffle.consolidateFiles` → ext4나 XFS 파일 시스템을 사용한다면 이 값을 true로 변경(default = false)로 하는 것이 좋다.
        - 셔플링에 쓸 메모리 리소스의 제한 여부: `spark.shuffle.spill` → 메모리를 제한하면 스파크는 제한 임계치를 초과한 데이터를 디스크로 내보낸다. 메모리 임계치를 너무 높게 설정하면 메모리 부족 예외(out-of-memory exception)이 발생할 수 있다. 반대로 너무 낮게 설정하면 데이터를 자주 내보내므로 균형을 잘 맞추는 것이 중요하다.
        - `spark.shuffle.compress`: 중간 파일의 압축 여부를 지정할 수 있다.
        - `spark.shuffle.spill.batchSize`: 데이터를 디스크로 내보낼 때 일괄로 직렬화 또는 역직렬화할 객체 개수를 지정한다.
        - `spark.shuffle.service.port`: 외부 셔플링 서비스를 활성화할 경우 서비스 서버가 사용할 포트 번호를 지정한다.
- 파티션 단위로 데이터 매핑: RDD의 각 파티션에 개별적으로 매핑 함수를 적용할수도 있다. 이 메서드를 잘 활용하면 각 파티션 내에서만 데이터가 매핑되도록 기존 변환 연산자를 최적화해 셔플링을 억제할 수 있다.
    - `mapPartitions`와 `mapPartitionsWithIndex`: mapPartitions는 각 파티션의 모든 요소를 반복문으로 처리하고 새로운 RDD 파티션을 생성한다. mapPartitionWithIndex는 매핑 함수에 파티션 번호가 함께 전달된다.
    - `glom`: 각 파티션의 모든 요소를 배열 하나로 모으고, 이 배열들을 요소로 포함하는 새로운 RDD를 반환한다. 따라서 새로운 RDD에 포함된 요소 개수는 이 RDD의 파티션 개수와 동일하다. glom 연산자는 기존의 Partitioner를 제거한다.

        ```python
        import random
        l = [random.randrange(100) for x in range(500)]
        rdd = sc.parallelize(l, 30).glom() #30개의 파티션으로 나눠져 있는 것을 모음. 
        rdd.collect()
        rdd.count() #30
        ```
  </details>
  <details close>
  <summary>4.3. Joining, sorting, and grouping data</summary>
    
* **데이터 조인(Join)** : 스파크에서는 RDBMS의 고전적인 join 연산자뿐만 아니라, zip, cartesian, intersection 등 다양한 변환 연산자를 사용해 여러 RDD 내용을 합칠 수 있다.

    <img width="621" alt="Screen Shot 2021-06-20 at 9 57 57 PM" src="https://user-images.githubusercontent.com/43725183/122675025-940c2400-d212-11eb-9db3-769ed31b6e25.png">

    RDD의 key를 product ID로 변경

    - RDBMS와 유사한 Join 연산자: `join`(RDBMS의 inner join과 동일), `leftOuterJoin`, `rightOuterJoin`, `fullOuterJoin` → 이 연산자들은 PairRDD에서만 사용할 수 있다. Partitioner 객체나 파티션 개수를 전달할 수 있다.

        ```python
        totalsByProd = transByProd.mapValues(lambda t: float(t[5])).reduceByKey(lambda tot1, tot2: tot1 + tot2)

        products = sc.textFile("first-edition/ch04/ch04_data_products.txt").map(lambda line: line.split("#")).map(lambda p: (int(p[0]), p))
        totalsAndProds = totalsByProd.join(products)
        totalsAndProds.first()
        #(68, (62133.899999999994, ['68', 'Niacin', '6295.48', '1']))
        #68은 key(productID), 그리고 value의 첫번째는 totalByProd의 값, 두번째 리스트는 products 정보.
        ```

        ```python
        totalsWithMissingProds = products.leftOuterJoin(totalsByProd)
        missingProds = totalsWithMissingProds.filter(lambda x: x[1][1] is None).map(lambda x: x[1][0])
        missingProds.foreach(lambda p: print(", ".join(p)))
        #3, Cute baby doll, battery, 1808.79, 2
        #43, Tomb Raider PC, 2718.14, 1
        #63, Pajamas, 8131.85, 3
        #20, LEGO Elves, 4589.79, 4
        #어제 판매하지 않은 상품 리스트 출력. 
        ```

    - `subtract`이나 `subtractByKey` 변환 연산자로 공통 값 제거: `substract`은 첫번째 RDD에서 두번째 RDD의 요소를 제거한 여집합을 반환한다. 이는 일반 RDD에서도 사용할 수 있으며, 각 요소의 키나 값만 비교하는 것이 아니라 요소 전체를 비교해 제거여부를 판단한다. 
    반면 `substracByKey`는 PairRDD에서만 사용할 수 있으며 키와 값을 모두 보는 것이 아니라 키만 본다.

        ```python
        missingProds = products.subtractByKey(totalsByProd)
        missingProds.foreach(lambda p: print(", ".join(p[1])))
        #leftOuterJoin후 None 값인 record만 뽑아내는 과정대신 substractByKey를 사용하면 된다. 
        ```

    - `cogroup` 변환 연산자로 RDD 조인: `cogroup`은 여러 RDD 값을 각각 키로 그루핑한 후 키를 기준으로 조인한다. `cogroup`은 RDD를 최대 세개까지 조인할 수 있는데, 단 `cogroup`을 호출한 RDD와 `cogroup`에 전달된 RDD는 모두 동일한 타입의 키를 가져야 한다.

        ```python
        prodTotCogroup = totalsByProd.cogroup(products)
        prodTotCogroup.first()
        #(68, (<pyspark.resultiterable.ResultIterable object at 0x107d3ab90>, <pyspark.resultiterable.ResultIterable object at 0x107da1fd0>))

        prodTotCogroup.filter(lambda x: len(x[1][0].data) == 0).foreach(lambda x: print(", ".join(x[1][1].data[0])))
        #total 가격과 product 정보의 outer join. 두 RDD 중 한쪽에만 등장한 키의 경우 다른 쪽 RDD iterator는 비어 있다. 
        ```

    - `intersection` 변환 연산자 사용: `intersection`은 교집합을 찾는다.

        ```python
        totalsByProd.map(lambda t: t[0]).intersection(products.map(lambda p: p[0]))
        ```

    - `cartesian` 변환 연산자로 RDD 두개 결합: 두 RDD의 데카르트 곱을 계산한다(요소의 모든 combination return). 따라서 이를 이용해 두 RDD 요소들을 서로 비교하는데도 사용할 수 있다.

        ```python
        rdd1 = sc.parallelize([7,8,9])
        rdd2 = sc.parallelize([1,2,3])
        rdd1.cartesian(rdd2).collect()
        #[(7, 1), (7, 2), (7, 3), (8, 1), (8, 2), (8, 3), (9, 1), (9, 2), (9, 3)]
        rdd1.cartesian(rdd2).filter(lambda el: el[0] % el[1] == 0).collect()
        #rdd2숫자로 rdd1 숫자를 나머지 없이 나눌 수 있는 조합 찾기
        [(7, 1), (8, 1), (8, 2), (9, 1), (9, 3)]
        ```

    - `zip` 변환 연산자로 RDD 조인: 모든 RDD에 사용가능. 첫번째 쌍은 각 RDD의 첫번째 요소를 조합한 것이며, 두번째 쌍은 두 번째 요소를 조합하고, 세 번째 쌍은 세 번째 요소를 조합하는 식으로 끝까지 이어진다. → 두 RDD의 파티션 개수가 다르거나 두 RDD의 모든 파티션이 서로 동일한 개수의 요소를 포함하지 않으면 오류를 발생한다.

        ```python
        rdd1 = sc.parallelize([1,2,3])
        rdd2 = sc.parallelize(["n4","n5","n6"])
        rdd1.zip(rdd2).collect()
        #[(1, 'n4'), (2, 'n5'), (3, 'n6')]
        ```

    - `zipPartitions` 변환 연산자로 RDD 조인: 모든 RDD에 사용가능. RDD는 서로 파티션 개수가 동일해야하지만, 모든 파티션이 서로 동일한 개수의 요소를 가져야 한다는 조건을 꼭 만족하지 않아도 된다. → 아직 python에서는 사용할 수 없다.
- **데이터 정렬(sorting)**
    - `repartitionAndSortWithinPartition`(4.2 절 참조), `sortByKey`, `sortBy`을 사용해서 정렬할 수 있다.

        ```python
        sortedProds = totalsAndProds.sortBy(lambda t: t[1][1][1])
        sortedProds.collect()
        ```

        이 결과는 `totalsAndProds` RDD에서 상품 이름을 키로 끄집어낸 후 `sortByKey`를 호출해서도 확인할 수 있다. 

    - 정렬 가능한 클래스 생성하기
    - 이차 정렬: 파티션을 2개로 바꾸고, 각 파티션 내에서 요소를 정렬한다.

        ```python
        rdd = sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)])
        rdd2 = rdd.repartitionAndSortWithinPartitions(2)
        rdd2.glom().collect()
        #[[(0, 5), (0, 8), (2, 6)], [(1, 3), (3, 8), (3, 8)]]
        #파티션1: [(0, 5), (0, 8), (2, 6)]
        #파티션2: [(1, 3), (3, 8), (3, 8)]
        ```
        <img width="300" alt="Screen Shot 2021-06-20 at 9 59 42 PM" src="https://user-images.githubusercontent.com/43725183/122675077-d46ba200-d212-11eb-852c-ac15c055cf3c.png">


    - `top`과 `takeOrdered`로 정렬된 요소 가져오기: 이 두 메서드는 전체 데이터를 정렬하지 않는다. 그 대신 각 파티션에서 상위(또는 하위) n개 요소를 가져온 후 이 결과를 병합하고, 이 중 상위(또는 하위) n개 요소를 반환한다. → 두 메서드는 훨씬 더 적은 양의 데이터를 네트워크로 전송하며, sortBy와 take를 개별적으로 호출하는 것보다 무척 빠르다.
- **데이터 그루핑(grouping)**: 데이터 그루핑은 데이터를 특정 기준에 따라 단일 컬렉션으로 집계하는 연산을 의미한다.
    - `aggregateByKey`: 4.1.2절 참조
    - `groupBykey`(`groupBy`): groupByKey는 동일한 키를 가진 모든 요소를 단일 키-값 쌍으로 모은 Pair RDD를 반환한다. groupBy는 PairRDD가 아닌 일반 RDD에서도 사용할 수 있으며, 일반 RDD를 PairRDD로 변환하고 groupByKey를 호출하는 것과 같은 결과를 만들 수 있다. groupByKey는 각 키의 모든 값을 메모리로 가져오기 때문에 이 메서드를 사용할 때는 메모리 리소스를 과다하게 사용하지 않도록 주의해야 한다. 모든 값을 한꺼번에 그루핑할 필요가 없으면 aggregateByKey나 reduceByKey, foldByKey를 사용하는 것이 좋다.
    - `combineByKey`: combineByKey를 호출하려면 세 가지 커스텀 함수를 정의해 전달해야 한다. 첫번째 함수는 `createCombiner`로 각 파티션별로 키의 첫번째 값에서 최초 결합값을 생성하는 데 사용한다. 두번째 함수(`mergeValue`)는 Pair RDD에 저장된 값들을 결합값(combined value)로 병합하고(동일 파티션 내에서 해당 키의 다른 값을 결합 값에 추가해 병합하는데 사용), 세번째 함수(`mergeCombiner`)는 결합 값을 최종 결과를 병합(여러 파티션의 결합 값을 최종 결과로 병합)한다.

        ```python
        def createComb(t):
            total = float(t[5])   #구매 금액
            q = int(t[4])         #구매 수량
            return (total/q, total/q, q, total)  #상품 낱개의 가격을 계산해 최저가격과 최고 가격의 초깃값으로 사용.

        def mergeVal(p,t):
            mn,mx,c,tot = p
            total = float(t[5])
            q = int(t[4])
            return (min(mn,total/q),max(mx,total/q),c+q,tot+total)

        def mergeComb(p1, p2):
            mn1,mx1,c1,tot1 = p1
            mn2,mx2,c2,tot2 = p2
            return (min(mn1,mn1),max(mx1,mx2),c1+c2,tot1+tot2)

        avgByCust = transByCust.combineByKey(createComb, mergeVal, mergeComb).\
        mapValues(lambda t: (t[0], t[1], t[2], t[3], t[3]/t[2]))
        avgByCust.first()

        totalsAndProds.map(lambda p: p[1]).map(lambda x: ", ".join(x[1])+", "+str(x[0])).saveAsTextFile("ch04/ch04output-totalsPerProd")
        avgByCust.map(lambda (pid, (mn, mx, cnt, tot, avg)): "%d#%.2f#%.2f#%d#%.2f#%.2f" % (pid, mn, mx, cnt, tot, avg)).saveAsTextFile("ch04/ch04output-avgByCust")
        ```
  </details>
  <details close>
  <summary>4.4. Understanding RDD dependencies</summary>
    
* **RDD의 의존 관계(dependency)** : 스파크의 실행 모델은 **Directed Acyclic Graph(DAG)** 에 기반한다. 방향성(directed) 그래프는 간선이 한 정점에서 다른 정점을 가리키는 방향성이 있는 그래프를 의미한다. 그 중 DAG는 간선의 방향을 따라 이동했을 때 같은 정점에 두 번 이상 방문할 수 없도록 연결된 그래프를 의마한다. (이름 그대로 acyclic하다)
**스파크의 DAG는 RDD를 정점(vertex)로, RDD 의존관계를 간선(edge)로 정의한 그래프를 의미한다.** RDD의 변환 연산자를 호출할 때마다 새로운 정점(RDD)와 새로운 간선(의존관계)이 생성된다. 변환 연산자로 생성된 RDD가 이전 RDD에 의존하므로 간선방향은 자식 RDD(새 RDD)에서 부모 RDD(이전 RDD)로 향한다. 이러한 RDD 의존 관계 그래프를 **RDD lineage**라고 한다.
    - **좁은(narrow) 의존 관계**: 데이터를 다른 파티션으로 전송할 필요가 없는 변환 연산은 좁은 의존 관계를 형성한다.
        - one-to-one 의존관계: range 의존관계 이외의 모든 셔플링이 필요하지 않은 변환연산자로 생성.
        - range 의존관계: 여러 부모 RDD에 대한 의존 관계를 하나로 결합한 경우로 union 변환연산자만 해당.
    - **넓은(wide, 또는 shuffle) 의존 관계**: 셔플링을 수행할 때 형성된다. 참고로 RDD를 조인하면 셔플링이 항상 발생한다.<br><br>

    ```python
    import random
    l = [random.randrange(10) for x in range(500)]
    listrdd = sc.parallelize(l, 5)  #파티션 5개로 나누기
    pairs = listrdd.map(lambda x: (x, x*x))     

    reduced = pairs.reduceByKey(lambda v1, v2: v1+v2)

    finalrdd = reduced.mapPartitions(lambda itr: ["K="+str(k)+",V="+str(v) for (k,v) in itr])
    finalrdd.collect()
    print(finalrdd.toDebugString())
    #map은 좁은(one-to-one) 의존 관계를 만드는 연산자. reduceBykey는 넓은(셔플) 의존관계를 형성
    ```

    <img width="1000" alt="Screen Shot 2021-06-20 at 9 55 12 PM" src="https://user-images.githubusercontent.com/43725183/122674936-34157d80-d212-11eb-8c49-cca2da11a64f.png">



    ```
    //print(finalrdd.toDebugString())의 결과

    (5) PythonRDD[5] at collect at <stdin>:1 []
     |  MapPartitionsRDD[4] at mapPartitions at PythonRDD.scala:133 []
     |  ShuffledRDD[3] at partitionBy at NativeMethodAccessorImpl.java:0 []
     +-(5) PairwiseRDD[2] at reduceByKey at <stdin>:1 []
        |  PythonRDD[1] at reduceByKey at <stdin>:1 []
        |  ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:195 []
    ```
    5번 RDD가 가장 최근에 만들어진 RDD. ShuffledRDD가 출력된 지점을 곧 셔플링을 수행하는 시점으로 간주할 수 있다. → 이러한 출력 결과를 잘 활용하면 스파크 프로그램의 셔플링 횟수를 최소화할 수 있다. 
    <br>괄호안 숫자(5)는 해당 RDD의 파티션 개수를 의미한다. 

- **스파크의 stage와 task**: 스파크는 셔플링이 발생하는 지점을 기준으로 스파크 잡(job) 하나를 여러 스테이지(stage)로 나눈다.
    <img width="1000" alt="Screen Shot 2021-06-20 at 9 54 13 PM" src="https://user-images.githubusercontent.com/43725183/122674901-0f210a80-d212-11eb-92c4-cdab59d039b1.png">


    - **1번 stage**는 셔플링으로 이어지는 parallelize, map, reduceByKey 변환 연산들이 포함된다. 1번 스테이지 결과는 중간 파일의 형태로 실행자 머신의 로컬 디스크에 저장된다. **2번 stage**에서는 이 중간 파일의 데이터를 적절한 파티션으로 읽어 들인 후 두 번째 map 변환 연산자부터 마지막 collect 연산까지 실행한다.
    - 스파크는 각 stage와 partition별로 task를 생성해 실행자에게 전달한다. 스테이지가 셔플링으로 끝나는 경우 이 단계의 태스트를 **shuffle-map task**(i.e. 1번 stage의 task)라고 한다. 스테이지의 모든 태스크가 완료되면 드라이버는 다음 스테이지의 태스크를 생성하고 실행자에 전달한다. 이 과정은 마지막 스테이지 결과를 드라이버로 반환할때까지 계속한다. 마지막 스테이지에 생성된 태스크를 **result task**(i.e. 2번 stage의 task)라고 한다.
- **RDD의 체크포인트(checkpoint)**: 변환 연산자를 계속 이어 붙이면 RDD lineage가 제약 없이 길어질 수 있다. 따라서 스파크는 중간에 스냅샷으로 RDD를 스토리지에 보관할 수 있는 **checkpointing** 방법을 제공한다. 일부 노드에 장애가 발생해도 유실된 RDD 조각을 처음부터 다시 계산할 필요가 없고, 그 대신 스냅샷 지점부터 나머지 lineage를 다시 계산한다.
    - 체크포인팅을 실행하면 스파크는 RDD의 데이터뿐만 아니라 RDD의 lineage까지 모두 저장한다. 체크포인팅을 완료한 후에는 저장한 RDD를 다시 계산할 필요가 없으므로 해당 RDD의 의존 관계와 부모 RDD 정보를 삭제한다.
    - 체크 포인팅은 RDD의 checkpoint 메서드를 호출해 실행할 수 있다. 하지만 먼저 SparkContext.setCheckpointDir에 데이터를 저장할 디렉터리부터 지정해야 한다. checkpoint 메서드는 해당 RDD에 job이 실행되기 전에 호출해야 하며, 그 이후 체크포인팅을 실제로 완료하려면 RDD에 행동 연산자(Action) 등을 호출해 잡을 실행하고 RDD를 구체화(materialize)해야한다.
  </details>
  <details close>
  <summary>4.5. Using accumulators and broadcast variables to communicate with Spark executors</summary><br>
    
* **누적 변수(accumulator)** 로 실행자에서 데이터 가져오기: 누적 변수는 여러 실행자가 공유하는 변수로 값을 더하는 연산만 허용한다. 누적 변수는 태스크의 진행상황을 추적하는데 사용할 수 있다.

    ```python
    #accumulators in Python cannot be named
    acc = sc.accumulator(0)  #0은 initial value
    l = sc.parallelize(range(1000000))
    l.foreach(lambda x: acc.add(1))
    acc.value  #누적 변수 값 가져오기. 이 값은 오직 드라이버만 참조할 수 있다. 
    # 1000000  -> 1을 1000000번 더한 결과

    #exception occurs(executor가 참조한 경우)
    l.foreach(lambda x: acc.value)
    #"Accumulator.value cannot be accessed inside tasks"

    #accumulables are not supported in Python
    #accumulableCollections are not supported in Python
    ```

- **공유 변수(broadcast variable)** 로 실행자에 데이터 전송: 공유변수는 여러 클러스터 노드가 공동으로 사용할 수 있는 변수다. 공유변수는 누적변수와 달리 실행자가 수정할 수 없다. 오직 드라이버만이 공유변수를 생성하며, 실행자에서는 읽기 연산만 가능하다.
    - **실행자 대다수가 대용량의 데이터를 공용으로 사용할 때는 이 데이터를 공유 변수로 만드는 것이 좋다.(핵심 point1)** 보통은 드라이버에서 생성한 변수를 태스크에서 사용하면 스파크는 이 변수를 직렬화하고 태스크와 함께 실행자로 전송한다. 하지만 드라이버 프로그램은 동일한 변수를 여러잡에 걸쳐 재사용할 수 있고 잡 하나를 수행할 때도 여러 태스크가 동일한 실행자에 할당될 수 있으므로, 변수를 필요 이상으로 여러번 직렬화해 네트워크로 전송하는 상황이 발생할 수 있다. 이때는 데이터를 더욱 최적화된 방식으로 단 한 번만 전송하는 공유 변수를 사용하는 편이 좋다. (3.2절 예시 참조)
    - 공유 변수는 Broadcast 타입의 객체를 반환하는 SparkContext.broadcast(value) 메서드로 생성한다. value 인수에는 직렬화 가능한 모든 종류의 객체를 전달할 수 있다.
    - **공유변수값을 참조할 때는 항상 value 메서드를 사용해야 한다.(핵심 point2)** 그렇지 않고 공유 변수에 직접 접근하면 스파크는 이 변수를 자동으로 직렬화해 태스크와 함께 전송한다. 이렇게 하면 공유 변수를 사용하는 이유, 즉 성능상 이득을 완전히 잃어버리게 된다.
    - 더 이상 필요하지 않은 공유변수는 destroy를 호출해 완전히 삭제(실행자와 드라이버에서 제거)할 수 있다. 또는 unpersist 메서드를 호출해 공유 변수 값을 실행자의 캐시에서 제거할 수 있다.
    - 공유변수와 관련된 스파크 매개변수
        - `spark.broadcast.compress`: 공유 변수를 전송하기 전에 데이터를 압축할지 여부를 지정한다.
        - `spark.broadcast.blockSize`: 공유 변수를 전송하는데 사용하는 데이터 청크의 크기를 설정한다. 실전 테스트를 거쳐 도출한 기본 값(4096KB)를 그대로 유지하면 좋다.
        - `spark.python.worker.reuse`: 파이썬의 공유 변수 성능에 큰 영향을 주는 매개변수다. 워커를 재사용하지 않으면 각 태스크별로 공유 변수를 전송해야 한다. 기본값이 true를 유지하면 좋다.
  </details></blockquote>
</details>

## 2장 meet the spark family
<details close>
  <summary><b>chapter 5</b> sparkling queries with spark sql</summary>
    <blockquote>
    <details close>
      <summary>5.1. Working with DataFrames </summary>
    </details>
    <details close>
      <summary>5.2. Beyond DataFrames: introducing DataSets </summary>
    </details>
    <details close>
      <summary>5.3. Using SQL commands </summary>
    </details>
    <details close>
      <summary>5.4. Saving and loading DataFrame data </summary>
    </details>
     <details close>
      <summary>5.5. Catalyst optimizer </summary>
    </details>
    <details close>
      <summary>5.6. Performance improvements with Tungsten </summary>
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 6</b> ingesting data with spark streaming</summary>
  <blockquote>
    <details close>
      <summary>6.1. Writing Spark Streaming applications </summary>
    </details>
    <details close>
      <summary>6.2. Using external data sources </summary>
    </details>
    <details close>
      <summary>6.3. Performance of Spark Streaming jobs </summary>
    </details>
    <details close>
      <summary>6.4. Structured Streaming </summary>
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 7</b> getting smart with MLlib</summary><br>
  <blockquote>
    <details close>
    <summary>7.1 Introduction to machine learning</summary><br>
      
- 일반적은 머신러닝 프로젝트는 다음 단계로 진행된다.
    1. **데이터 수집**
    2. **데이터 정제 및 준비**: 머신러닝에 적합한 정형 포맷 외에도 다양한 비정형 포맷(텍스트, 이미지, 음성, 이진 데이터 등)으로 저장된 데이터가 필요할 때도 많다. 따라서 이러한 비정형 데이터를 수치형 특성 변수로 변환할 방법을 고안하고 적용해야 한다. 또 결측 데이터를 처리하고, 동일한 정보가 갖가지 다른 값으로 기록되어 있다면(예를 들어 VW와 폭스바겐은 같은 제조사를 의미) 이를 적절히 보완해야 한다. 마지막으로 특징 변수에 스케일링(scailing)을 적용해 데이터의 모든 차원을 비교 가능한 범위로 변환하기도 한다.
    3. **데이터 분석 및 특징 변수 추출**: 데이터 내 상관관계를 조사하고, 필요하다면 데이터를 시각화 한다(or 차원 축소). 다음으로 가장 적절한 머신러닝 알고리즘을 선택하고, 데이터를 훈련 데이터셋과 검증 데이터셋으로 나눈다. 또는 교차 검증(cross-validation) 기법을 활용할 수도 있다. 교차 검증은 데이터셋을 여러 훈련 데이터셋가 검증 데이터셋ㅇ로 계속 나누어 테스트하고 결과들의 평균을 계산한다. 
    4. **모델 훈련**: 입력 데이터를 기반으로 머신 러닝의 학습 알고리즘을 실행해 알고리즘의 매개변수들을 학습하도록 모델을 훈련시킨다. 
    5. **모델 평가**: 모델을 검증 데이터셋에 적용하고 몇 가지 기준으로 모델 성능을 평가한다. 모델 성능이 만족스럽지 못할 때는 더 많은 입력 데이터가 필요하거나 특징 변수를 추출한 방법을 바꾸어야 할 수도 있다. 또는 feature space를 변경하거나 다른 모델을 시도할 수 있다. 어떤 변경을 시도하든 1단계나 2단계로 돌아가야한다. 
    6. **모델 적용**: 마침내 완성된 모델을 웹 사이트의 운영 환경에 배포한다. 
- 머신러닝 알고리즘의 유형
    1. **지도학습(supervised learning)**: 레이블(label)이 포함된 데이터셋을 사용. 회귀와 분류.
    2. **비지도학습(unsupervised learning)**: 레이블이 주어지지 않음. 군집화
- 스파크를 활용한 머신러닝의 장점: (1)분산 처리, (2) 단일 API로 1~6번까지 처리 가능
    </details>
    <details close>
    <summary>7.2. Linear algebra in Spark</summary><br>
      
스파크는 (1) 로컬환경(분산 저장되지 않는 데이터)에서 사용할 수 있는 벡터 및 행렬 클래스와 (2) 분산 환경에서 사용할 수 있는 여러 행렬 클래스를 제공한다. 스파크의 분산 행렬을 사용하면 대규모 데이터의 선형 대수 연산을 여러 머신으로 분산해 처리할 수 있다. 

> 스파크는 희소벡터, 밀집 벡터, 희소 행렬, 밀집 행렬을 모두 지원한다. 희소벡터(또는 행렬)는 원소의 대다수가 0인 벡터(또는 행렬)을 의미한다. 이러한 데이터는 0이 아닌 원소의 위치와 값의 쌍으로 표현하는 것이 더 효율적이다. map 또는 파이썬 딕셔너리와 유사하다. 
반면 밀집벡터(또는 행렬)은 모든 데이터를 저장한다. 다시 말해 배열이나 리스트처럼 원소 위치를 따로 저장하지 않고, 모든 위치의 원소 값을 순차적으로 저장한다. 

1. 로컬 벡터와 로컬 행렬
    - 로컬 벡터 생성

        ```python
        from pyspark.mllib.linalg import Vectors, Vector

        #dense vector
        dv1 = Vectors.dense(5.0,6.0,7.0,8.0)
        dv2 = Vectors.dense([5.0,6.0,7.0,8.0])

        #sparse vector: 벡터 크기, 위치 배열, 값 배열을 인수로 전달
        sv = Vectors.sparse(4, [0,1,2,3], [5.0,6.0,7.0,8.0])

        dv2[2]   #특정 위치의 원소 가져오기 -> 7
        dv1.size   #벡터 크기 조회  -> 4
        dv2.toArray()   #numpy array로 전환
        ```
        <img width="589" alt="Screen Shot 2021-06-22 at 7 41 22 PM" src="https://user-images.githubusercontent.com/43725183/122911029-d445e080-d391-11eb-8621-728616ae3d20.png">



    - 로컬 벡터의 선형 대수 연산

        ```python
        dv1 + dv2   #DenseVector([10.0, 12.0, 14.0, 16.0])
        dv1.dot(dv2)  #174.0
        ```

    - 로컬 밀집 행렬, 로컬 희소 행렬 생성

        ```python
        from pyspark.mllib.linalg import Matrices

        #dense matrix: 행, 열, 데이터 -> columnwise하게 들어간다. 
        dm = Matrices.dense(2,3,[5.0,0.0,0.0,3.0,1.0,4.0])

        #sparse matrix: 원소 값을 CSC(compressed sparse column)포맷으로 전달
        #CRC(https://rfriend.tistory.com/551)
        sm = Matrices.sparse(2,3,[0,1,2,4], [0,1,0,1], [5.0,3.0,1.0,4.0])
        sm.toDense()
        dm.toSparse()
        dm[1,1]
        ```

2. **분산 행렬**: 분산 행렬은 여러 머신에 걸쳐 저장할 수 있고 대량의 행과 열로 구성할 수 있다. 
    - **RowMatrix**: 각 행을 vector 객체에 저장해 RDD를 구성한다.
    - **IndexedRowMatrix**: 행의 원소들을 담은 vector 객체와 이 행의 행렬 내 위치를 저장한다. 아래는 rm이라는 RowMatrix를 IndexedRowMatrix로 변환하는 방법이다.

        ```python
        from pyspark.mllib.linalg.distributed import IndexedRowMatrix, IndexedRow
        rmind = IndexedRowMatrix(rm.rows().zipWithIndex().map(lambda x: IndexedRow(x[1], x[0])))
        ```

    - **CoordinateMatrix**: 개별 원소 값과 해당 원소의 행렬 내 위치(i,j)를 저장한다. 하지만 이 방법으로는 데이터를 효율적으로 저장할 수 없으므로 CoordinateMatrix는 오직 희소 행렬을 저장할 때만 사용해야 한다.
    - **BlockMatrix**: 분산 행렬 간 덧셈 및 곱셈 연산을 지원한다. 행렬을 블록 여러개로 나누고, 각 블록의 원소들을 로컬 행렬의 형태로 저장한 후 이 블록의 전체 행렬 내 위치와 로컬 행렬 객체를 튜플로 구성한다. → ((i, j), Matrix)
    - **분산 행렬의 선형 대수 연산**: 스파크는 분산 행렬의 선형 대수 연산을 제한적으로 제공하므로 나머지 연산은 따로 구현해야 한다.
    </details>
    <details close>
    <summary>7.3. Linear regression</summary><br>
      
선형 회귀는 독립 변수와 목표 변수 사이에 **선형 관계**가 있다고 가정한다. 

- **단순 선형 회귀 (simple linear regression)**
    - model: <a href="https://www.codecogs.com/eqnedit.php?latex=h(x)&space;=&space;w_0&space;&plus;&space;w_1x" target="_blank"><img src="https://latex.codecogs.com/gif.latex?h(x)&space;=&space;w_0&space;&plus;&space;w_1x" title="h(x) = w_0 + w_1x" /></a><br>
    - loss(cost) function(=MSE): <a href="https://www.codecogs.com/eqnedit.php?latex=C(w_0,&space;w_1)&space;=&space;\frac{1}{m}\Sigma(h(x_i)-y_i)^2&space;=&space;\frac{1}{m}\Sigma(w_0&space;&plus;&space;w_1x&space;-y_i)^2" target="_blank"><img src="https://latex.codecogs.com/gif.latex?C(w_0,&space;w_1)&space;=&space;\frac{1}{m}\Sigma(h(x_i)-y_i)^2&space;=&space;\frac{1}{m}\Sigma(w_0&space;&plus;&space;w_1x&space;-y_i)^2" title="C(w_0, w_1) = \frac{1}{m}\Sigma(h(x_i)-y_i)^2 = \frac{1}{m}\Sigma(w_0 + w_1x -y_i)^2" /></a><br>
    - loss function이 최소값을 갖는 <a href="https://www.codecogs.com/eqnedit.php?latex=w_0,&space;w_1" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_0,&space;w_1" title="w_0, w_1" /></a>을 찾는다. → **최적화 과정**
- **다중 선형 회귀 (multiple linear regression)**
    - model: <a href="https://www.codecogs.com/eqnedit.php?latex=h(X)&space;=&space;w_0&space;&plus;&space;w_1x_1&space;&plus;&space;w_2x_2&space;&plus;...&plus;&space;w_nx_n&space;=&space;W^TX" target="_blank"><img src="https://latex.codecogs.com/gif.latex?h(X)&space;=&space;w_0&space;&plus;&space;w_1x_1&space;&plus;&space;w_2x_2&space;&plus;...&plus;&space;w_nx_n&space;=&space;W^TX" title="h(X) = w_0 + w_1x_1 + w_2x_2 +...+ w_nx_n = W^TX" /></a> → 여기서 <a href="https://www.codecogs.com/eqnedit.php?latex=X^T&space;=&space;[1&space;\&space;x_1&space;\&space;...&space;\&space;x_n]" target="_blank"><img src="https://latex.codecogs.com/gif.latex?X^T&space;=&space;[1&space;\&space;x_1&space;\&space;...&space;\&space;x_n]" title="X^T = [1 \ x_1 \ ... \ x_n]" /></a>(즉, 상수 1 추가)
    - loss function: <a href="https://www.codecogs.com/eqnedit.php?latex=C(W)&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2" target="_blank"><img src="https://latex.codecogs.com/gif.latex?C(W)&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2" title="C(W) = \frac{1}{m}\Sigma((W^TX)_i-y_i)^2" /></a>
    - loss function이 최소값을 갖는 W을 찾는다. → **최적화 과정**
        - **정규방정식**으로 찾기: C(W)를 편미분한 값이 0이 되는 W를 계산 
        → <a href="https://www.codecogs.com/eqnedit.php?latex=W&space;=&space;(X^TX)^{-1}X^TY" target="_blank"><img src="https://latex.codecogs.com/gif.latex?W&space;=&space;(X^TX)^{-1}X^TY" title="W = (X^TX)^{-1}X^TY" /></a>
        - 경사 하강법으로 찾기: 정규 방정식의 해를 찾는 작업은 매우 많은 계산량이 필요하다. 특히 데이터셋의 차원이 크고 행이 많을수록 정규 방정식은 사용하기 어렵다. 따라서 정규 방정식보다는 **경사 하강법(gradient-descent method)** 를 더 널리 사용한다.
            1. <a href="https://www.codecogs.com/eqnedit.php?latex=w_0,&space;w_1,&space;...,&space;w_n" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_0,&space;w_1,&space;...,&space;w_n" title="w_0, w_1, ..., w_n" /></a>에 임의의 값을 할당
            2. <a href="https://www.codecogs.com/eqnedit.php?latex=w_j" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_j" title="w_j" /></a>별로 loss function의 편도함수를 계산. → <a href="https://www.codecogs.com/eqnedit.php?latex=\frac{\delta}{\delta&space;w_j}C(W)" target="_blank"><img src="https://latex.codecogs.com/gif.latex?\frac{\delta}{\delta&space;w_j}C(W)" title="\frac{\delta}{\delta w_j}C(W)" /></a>
            3. <a href="https://www.codecogs.com/eqnedit.php?latex=w_j" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_j" title="w_j" /></a>값을 갱신한다. → <a href="https://www.codecogs.com/eqnedit.php?latex=w_j&space;:=&space;w_j&space;-&space;\gamma\frac{\delta}{\delta&space;w_j}C(W)" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_j&space;:=&space;w_j&space;-&space;\gamma\frac{\delta}{\delta&space;w_j}C(W)" title="w_j := w_j - \gamma\frac{\delta}{\delta w_j}C(W)" /></a>, for every j
            (편미분 값이 음수 ↔ <a href="https://www.codecogs.com/eqnedit.php?latex=w_j" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_j" title="w_j" /></a>가 증가하게 갱신됨 ↔ loss function 값이 작아지는 방향)
            (<a href="https://www.codecogs.com/eqnedit.php?latex=\gamma" target="_blank"><img src="https://latex.codecogs.com/gif.latex?\gamma" title="\gamma" /></a>는 gradient descent의 stability를 높일 수 있는 step-size 매개변수이다. )
            4. 2, 3 과정을 반복하다가 loss function 값이 일정 tolerance value보다 작으면 알고리즘이 수렴(즉, 비용함수의 결과 값이 안정적일 때까지)했다고 판단하고 반복을 멈춤.
    </details>
    <details close>
    <summary>7.4. Analyzing and preparing the data</summary>

1. 데이터 불러오기: 파티션을 6개로 함 → RowMatrix로 변경.

    ```python
    from pyspark.mllib.linalg import Vectors, Vector
    housingLines = sc.textFile("ch07/housing.data", 6)
    housingVals = housingLines.map(lambda x: Vectors.dense([float(v.strip()) for v in x.split(",")]))

    from pyspark.mllib.linalg.distributed import RowMatrix
    housingMat = RowMatrix(housingVals)
    ```

2. EDA
    - 분포 확인: `Statistics.colStats(housingVals)`에는 각 열별 평균값, 최대값, 최솟값등을 제공한다.

        ```python
        from pyspark.mllib.stat._statistics import Statistics
        housingStats = Statistics.colStats(housingVals)
        housingStats.min()
        ```

    - 열 코사인 유사도(column cosine similarity) 분석: 두 열을 벡터 두 개로 간주하고 이들 사이의 각도를 구한 값이다.

        ```python
        housingColSims = housingMat.columnSimilarities()
        housingColSims.entries.foreach(lambda x: print(x))
        ```

        upper-triangular matrix가 반환되고, 열 유사도 값은 -1~ 1값을 가진다. 유사도 값이 -1이면 두 열의 방향이 완전히 반대고, 0이면 직각을 이룬다는 의미고, 1이면 두 열의 방향이 같다. 

    - 공분산 행렬 계산: `corr`(org.apache.spark.mllib.stat.Statistics)를 사용하면 스피어만 상관계수 또는 피어슨 상관계수도 구할 수 있다.

        ```python
        housingCovar = housingMat.computeCovariance()
        ```

3. preprocessing
    - 레이블 포인트로 변환: `LabeledPoint`는 거의 모든 스파크 머신 러닝 ㅇㄹ고리즘에 사용하는 객체로, 목표 변수 값과 특징 변수 벡터로 구성된다. 현재 데이터 셋에서 목표 변수는 마지막 칼럼에 있다.

        ```python
        from pyspark.mllib.regression import LabeledPoint
        def toLabeledPoint(x):
          a = x.toArray()
          return LabeledPoint(a[-1], Vectors.dense(a[0:-1]))

        housingData = housingVals.map(toLabeledPoint)
        ```

    - 데이터 분할: training 데이터셋은 모델 훈련에 사용하고, validation 데이터셋은 훈련에 사용하지 않은 데이터에서도 모델이 얼마나 잘 작동하는지 확인하는 용도로 사용한다. 일반적으로 8:2의 비율로 분할한다.

        ```python
        sets = housingData.randomSplit([0.8, 0.2])
        housingTrain = sets[0]
        housingValid = sets[1]
        ```

    - 특징 변수 스케일링 및 평균 정규화: feature scaling은 데이터 범위를 비슷한 크기로 조정하는 작업이고, normalization은 평균이 0에 가깝도록 데이터를 옮기는 작업이다. 이 두작업을 한번에 실행하려면 StandardScaler가 필요하다.

        ```python
        from pyspark.mllib.feature import StandardScaler
        scaler = StandardScaler(True, True).fit(housingTrain.map(lambda x: x.features))

        trainLabel = housingTrain.map(lambda x: x.label)
        trainFeatures = housingTrain.map(lambda x: x.features)
        validLabel = housingValid.map(lambda x: x.label)
        validFeatures = housingValid.map(lambda x: x.features)

        trainScaled = trainLabel.zip(scaler.transform(trainFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
        validScaled = validLabel.zip(scaler.transform(validFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
        ```

        StandardScaler 학습 단계에서는 훈련 데이터셋만 사용하도록 한다. 머신러닝 모델이 미리 습득할 수 있는 정보는 훈련 데이터셋에만 한정해야 하므로 표준화 모델을 학습할 때도 훈련 데이터셋만 사용해야 한다.
    </details>
    <details close>
    <summary>7.5. Fitting and using a linear regression model</summary>
      
- model fitting: 머신러닝 알고리즘을 포함한 반복 알고리즘들은 같은 데이터를 여러번 재사용하기 때문에 캐시를 꼭 활용해야한다.

    ```python
    from pyspark.mllib.regression import LinearRegressionWithSGD
    alg = LinearRegressionWithSGD()
    trainScaled.cache()
    validScaled.cache()
    model = alg.train(trainScaled, iterations=200, intercept=True)
    ```

- prediction: `validPredicts`에는 예측값과 실제 Label이 함께 저장된다.

    ```python
    validPredicts = validScaled.map(lambda x: (float(model.predict(x.features)), x.label))
    validPredicts.collect()

    import math
    RMSE = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
    #4.292515313462585
    ```

    Root Mean Squared Error(RMSE)를 계산하면 4.29이다. 

- model evaluation: RMSE외에도 MAE, R square(모델이 예측하는 목표 변수의 변화량을 설명하는 정도와 설명하지 못하는 정도를 나타내는 척도. 이 값이 1에 가깝다면 모델이 목표 변수가 가진 분산의 많은 부분을 설명할 수 있다는 의미), explainedVariance(R square와 유사한 지표)등을 산출할 수 있다.

    ```python
    from pyspark.mllib.evaluation import RegressionMetrics
    validMetrics = RegressionMetrics(validPredicts)
    validMetrics.rootMeanSquaredError  #4.292515313462585
    validMetrics.meanSquaredError  #18.42568771631079
    validMetrics.r2   #0.7632157714756399
    ```

- 모델 매개변수 해석: coefficient 확인. 특정 가중치(weight, coefficient)값이 0에 가깝다면 해당 차원은 목표 변수에 크게 기여하지 못한다는 의미(스케일링은 진행한 경우에)

    ```python
    import operator
    print("\n".join([str(s) for s in sorted(enumerate([abs(x) for x in model.weights.toArray()]), key=operator.itemgetter(0))]))
    #(0, 1.000538921034109)
    #(1, 1.1273180677794499)
    #(2, 0.24809539149291654)
    #(3, 0.7864653560485336)
    #(4, 1.846301136999396)
    #(5, 2.498899558304125)
    #(6, 0.20040482761985576)
    #(7, 3.3293179912270845)
    #(8, 1.9227159026529925)
    #(9, 0.8581928817743215)
    #(10, 1.9938882718949151)
    #(11, 1.0187427812723926)
    #(12, 3.9936868478891263)
    ```

    12번(LSTAT, 저소득층 인구 비율)컬럼과 7번(DIS, 보스턴 고용 센터 다섯 군데까지의 가중 거리)컬럼의 영향력이 큰 것으로 나타났다. 

- 모델의 저장 및 불러오기: 스파크는 학습된 모델을 Parquet 파일 포맷으로 파일 시스템에 저장하고, 추후 다시 로드할 수 있는 방법을 제공한다. 스파크는 지정된 경로에 새로운 디렉터리를 생성하고 모델의 **데이터 파일**(모델의 weight와 intercept을 저장)과 **메타데이터 파일**(모델을 구현한 클래스 이름, 모델 파일의 버전, 모델에 사용된 특징 변수의 개수)을 Parquet 포맷으로 저장한다.

    ```python
    #저장하기
    model.save(sc, "ch07/ch07output/model")

    #다시 모델 불러오기
    from pyspark.mllib.regression import LinearRegressionModel
    model = LinearRegressionModel.load(sc, "ch07/ch07output/model")
    ```
    </details>
    <details close>
    <summary>7.6. Tweaking the algorithm</summary>
      
1. 적절한 **감마 값(이동거리)** 와 **반복 횟수(iteration)** 을 찾는 방법
    - 감마값이 너무 작으면 경사를 내려가는 거리가 짧으므로 알고리즘이 수렴하려면 더 많은 반복 단계를 거쳐야 한다. 반대로 너무 크면 알고리즘이 수렴하지 않을 수 있다(아래예시에서 3인 경우). 적절한 이동거리는 데이터셋에 따라 다르다.
    - iteration 수가 너무 크면 알고리즘 학습시간이 너무 오래 걸리고, 너무 적으면 최저점에 도달하지 못할 수 있다(→ 아래 예시에서 600번 반복을 한다고 해서 더 나은 결과를 보여주지는 않는다).
    - 이동거리와 반복 횟수의 최적 값을 찾는 방법 중 하나는 값의 여러 조합을 하나씩 실험해보고 가장 좋은 결과를 내는 조합을 고르는 것이다.

        ```python
        def iterateLRwSGD(iterNums, stepSizes, train, valid):
          from pyspark.mllib.regression import LinearRegressionWithSGD
          import math
          for numIter in iterNums:
            for step in stepSizes:
              alg = LinearRegressionWithSGD()
              model = alg.train(train, iterations=numIter, step=step, intercept=True)
              rescaledPredicts = train.map(lambda x: (float(model.predict(x.features)), x.label))
              validPredicts = valid.map(lambda x: (float(model.predict(x.features)), x.label))
              meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              print("%d, %5.3f -> %.4f, %.4f" % (numIter, step, meanSquared, meanSquaredValid))
              #Uncomment if you wish to see weghts and intercept values:
              #print("%d, %4.2f -> %.4f, %.4f (%s, %f)" % (numIter, step, meanSquared, meanSquaredValid, model.weights, model.intercept))

        iterateLRwSGD([200, 400, 600], [0.05, 0.1, 0.5, 1, 1.5, 2, 3], trainScaled, validScaled)
        # 200, 0.050 -> 7.5420, 7.4786
        # 200, 0.100 -> 5.0437, 5.0910
        # 200, 0.500 -> 4.6920, 4.7814
        # 200, 1.000 -> 4.6777, 4.7756
        # 200, 1.500 -> 4.6751, 4.7761
        # 200, 2.000 -> 4.6746, 4.7771
        # 200, 3.000 -> 108738480856.3940, 122956877593.1419
        # 400, 0.050 -> 5.8161, 5.8254
        # 400, 0.100 -> 4.8069, 4.8689
        # 400, 0.500 -> 4.6826, 4.7772
        # 400, 1.000 -> 4.6753, 4.7760
        # 400, 1.500 -> 4.6746, 4.7774
        # 400, 2.000 -> 4.6745, 4.7780
        # 400, 3.000 -> 25240554554.3096, 30621674955.1730
        # 600, 0.050 -> 5.2510, 5.2877
        # 600, 0.100 -> 4.7667, 4.8332
        # 600, 0.500 -> 4.6792, 4.7759
        # 600, 1.000 -> 4.6748, 4.7767
        # 600, 1.500 -> 4.6745, 4.7779
        # 600, 2.000 -> 4.6745, 4.7783
        # 600, 3.000 -> 4977766834.6285, 6036973314.0450
        ```

        → 대부분 검증 데이터셋의 RMSE는 훈련 RMSE보다 크다 (overfitting)

2. **고차 다항식 추가**: 스파크는 고차 다항식을 사용한 비선형 회귀모델을 지원하지 않는다. 그 대신 기존 변수의 제곱값을 추가해 데이터셋을 확장해서 비슷한 효과를 낼 수 있다. 또한 비슷하게 두 변수가 공동으로 영향을 줄 때는 $x_1*x_2$와 같은 상호작용항(interaction term)을 추가하는 것도 도움이 된다. → 위의 예시에서 고차 다항식을 추가했을때 RMSE자체는 감소하였다. (but, RMSE가 감소했다고 무조건 좋은 것인가?)

    ```python
    def addHighPols(v):
      import itertools
      a = [[x, x*x] for x in v.toArray()]
      return Vectors.dense(list(itertools.chain(*a)))

    housingHP = housingData.map(lambda v: LabeledPoint(v.label, addHighPols(v.features)))
    len(housingHP.first().features)  #26

    setsHP = housingHP.randomSplit([0.8, 0.2])
    housingHPTrain = setsHP[0]
    housingHPValid = setsHP[1]
    scalerHP = StandardScaler(True, True).fit(housingHPTrain.map(lambda x: x.features))
    trainHPLabel = housingHPTrain.map(lambda x: x.label)
    trainHPFeatures = housingHPTrain.map(lambda x: x.features)
    validHPLabel = housingHPValid.map(lambda x: x.label)
    validHPFeatures = housingHPValid.map(lambda x: x.features)
    trainHPScaled = trainHPLabel.zip(scalerHP.transform(trainHPFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
    validHPScaled = validHPLabel.zip(scalerHP.transform(validHPFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
    trainHPScaled.cache()
    validHPScaled.cache()

    iterateLRwSGD([200, 400], [0.4, 0.5, 0.6, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5], trainHPScaled, validHPScaled)
    #이동거리가 1.1이고 반복횟수가 400회일때 결과가 가장 좋다(# 400, 1.100 -> 4.0378, 3.9836)

    iterateLRwSGD([200, 400, 800, 1000, 3000, 6000], [1.1], trainHPScaled, validHPScaled)
    #이동거리는 고정하고 반복횟수만 늘려보자. 
    # 200, 1.100 -> 4.1605, 4.0108
    # 400, 1.100 -> 4.0378, 3.9836
    # 800, 1.100 -> 3.9438, 3.9901
    # 1000, 1.100 -> 3.9199, 3.9982
    # 3000, 1.100 -> 3.8332, 4.0633
    # 6000, 1.100 -> 3.7915, 4.1138
    #오히려 RMSE가 증가한다.
    ```

3. **편향-분산 상충 관계와 모델의 복잡도(bias-variance tradeoff)**: 머신러닝 모델은 훈련 데이터셋의 데이터를 잘 학습해야 하면서 현재까지 관찰되지 않은 다른 데이터에서도 좋은 성능을 낼 수 있는 확장성 또한 갖추어야 한다. 그러나 두 가지 목표를 완벽하게 이룰 수는 없다. 
    - bias가 크다(모델을 바라보는 관점이 편향되었다. )
     → **underfitting** → 모델을 더 복잡하게 만들어야 한다.
    - variance가 크다(예측값의 변동 폭이 더 크다)
     → **overfitteing** → 모델을 덜 복잡하게 만들어야 한다.
4. 잔차 차트 그리기(**residual plot**): 모델의 복잡도를 어디까지 올려야 할지 잔차 차트를 통해 결정해보자. residual은 훈련 데이터셋의 각 예제별 실제 레이블 값과 모델이 예상한 레이블 값 차이이다. 잔차 차트는 X 축에 예측값을 설정하고 Y 축에 잔차 값을 설정해 그린다.
→ 이상적인 잔차 차트에는 눈에 띄는 패턴이 없어야 한다. X축의 모든 점이 거의 동일한 Y 값을 가져야 하며, 점들을 가로지르는 최적선을 그렸을때 선이 거의 평평해야 한다. 반면 최적선이 U자 같은 모양을 보인다면 이는 일부 차원에서 비선형 모델이 더 적절할 수 있다는 의미다. 
→ 자세한 잔차 차트 내용은 책의 범위를 벗어나므로 설명하지 않는다. 모델 성능에 대한 자세한 정보를 얻으려면 잔차 차트를 연구하는데 많은 노력을 기울여야 한다. 
5. **규제화(regularization)**를 사용해 과적합 방지: 과적합을 방지하는 한 가지 방법으로 모델 매개변수 값이 클수록 불이익을 가해 모델 편향을 키우고 분산을 낮추는 regularization 기법을 사용하자. 

    →  <a href="https://www.codecogs.com/eqnedit.php?latex=C(W)&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2&space;-&space;\beta&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2&space;-\lambda||w||" target="_blank"><img src="https://latex.codecogs.com/gif.latex?C(W)&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2&space;-&space;\beta&space;=&space;\frac{1}{m}\Sigma((W^TX)_i-y_i)^2&space;-\lambda||w||" title="C(W) = \frac{1}{m}\Sigma((W^TX)_i-y_i)^2 - \beta = \frac{1}{m}\Sigma((W^TX)_i-y_i)^2 -\lambda||w||" /></a>

    여기서 람다는 일반화 매개변수이고 여기에 가중치 벡터의 L1 norm(=$||w||_1$ → 벡터요소의 절대값을 합한 값) 또는 L2 norm(=$||w||_2$→ 벡터의 길이)을 곱해준다. 

    - Lasso regression(L1) : 개별 가중치는 0으로 만들어 변수를 데이터셋에서 완전히 제거한다.

        ```python
        def iterateLasso(iterNums, stepSizes, regParam, train, valid):
          from pyspark.mllib.regression import LassoWithSGD
          for numIter in iterNums:
            for step in stepSizes:
              alg = LassoWithSGD()
              model = alg.train(train, intercept=True, iterations=numIter, step=step, regParam=regParam)
              rescaledPredicts = train.map(lambda x: (model.predict(x.features), x.label))
              validPredicts = valid.map(lambda x: (model.predict(x.features), x.label))
              meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              print("%d, %5.3f -> %.4f, %.4f" % (numIter, step, meanSquared, meanSquaredValid))
              #print("\tweights: %s" % model.weights)

        iterateLasso([200, 400, 1000, 3000, 6000, 10000, 15000], [1.1], 0.01, trainHPScaled, validHPScaled)
        #Our results:
        # 200, 1.100 -> 4.1762, 4.0223
        # 400, 1.100 -> 4.0632, 3.9964
        # 1000, 1.100 -> 3.9496, 3.9987
        # 3000, 1.100 -> 3.8636, 4.0362
        # 6000, 1.100 -> 3.8239, 4.0705
        # 10000, 1.100 -> 3.7985, 4.1014
        # 15000, 1.100 -> 3.7806, 4.1304
        ```

    - Ridge regression(L2)

        ```python
        def iterateRidge(iterNums, stepSizes, regParam, train, valid):
          from pyspark.mllib.regression import RidgeRegressionWithSGD
          import math
          for numIter in iterNums:
            for step in stepSizes:
              alg = RidgeRegressionWithSGD()
              model = alg.train(train, intercept=True, regParam=regParam, iterations=numIter, step=step)
              rescaledPredicts = train.map(lambda x: (model.predict(x.features), x.label))
              validPredicts = valid.map(lambda x: (model.predict(x.features), x.label))
              meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
              print("%d, %5.3f -> %.4f, %.4f" % (numIter, step, meanSquared, meanSquaredValid))

        iterateRidge([200, 400, 1000, 3000, 6000, 10000], [1.1], 0.01, trainHPScaled, validHPScaled)
        # Our results:
        # 200, 1.100 -> 4.2354, 4.0095
        # 400, 1.100 -> 4.1355, 3.9790
        # 1000, 1.100 -> 4.0425, 3.9661
        # 3000, 1.100 -> 3.9842, 3.9695
        # 6000, 1.100 -> 3.9674, 3.9728
        # 10000, 1.100 -> 3.9607, 3.9745
        ```

    → 일반화 기법은 모델 과적합 현상을 완화할 수 있다. 일반화 매개변수($\beta$)가 증가할수록 모델 과적합은 감소한다. 또, 일반화 기법은 모델 성능에 크게 기여하지 못하는 차원들의 영향력을 감소시킬 수 있으므로, 데이터셋의 차원이 매우 많은 상황에서도 오차를 더 빠르게 최적화할 수 있다. 그러나 일반화 매개변수의 최적 값 또한 찾아야 하기 때문에 모델 훈련과정을 더 복잡하게 만든다는 단점은 있다. 

6. **k-겹 교차 검증(k-fold cross-validation)**: 전체 데이터셋을 동일한 크기의 k개 부분 집합으로 나눈다. 그런 다음 각 부분 집합을 전체 데이터셋에서 제외해 총 k개의 훈련 데이터셋을 만들고, 이를 사용해 모델 k개를 훈련시킨다. 각 모델의 훈련 데이터셋에서 제외된 부분 집합은 해당 모델의 검증 데이터셋으로 사용하며, 나머지 부분 집합은 훈련 데이터셋으로 사용한다. 
→ 먼저 변수 값의 각 조합별로 모델 k개를 모두 학습시키고, 이 모델들이 기록한 **오차 평균을 계산**한 후 마지막으로 가장 작은 평균 오류를 기록한 변수 값의 조합을 선택하는 것이다.  
→ k-겹 교차 검증이 중요한 이유는 어떤 훈련 데이터셋과 검증 데이터셋을 사용했느냐에 따라 모델 학습 결과가 크게 달라지기 때문이다.
    </details>
    <details close>
    <summary>7.7. Optimizing linear regression</summary>
      
선형 회귀 알고리즘이 loss function의 최저점을 더 빨리 찾을 수 있는 두 가지 방법을 추가로 알아보자.

1. 미니배치 기반 확률적 경사 하강법
    - **Batch Gradient Descent(BGD)**: 먼저 설명한 경사 하강법의 각 단계에서는 전체 데이터셋을 한꺼번에 사용해 오차를 계산하고 가중치 값을 갱신했다. 이러한 방법을 BGD라고 한다.
    - 반면, 미니배치 기반 확률적 경사 하강법은 각 반복 단계에서 데이터셋의 일부분(k개, 여기서 k는 전체 샘플 수보다 작은 수)만 사용한다. k값이 1인 경우(즉, 알고리즘이 각 반복단계에서 오직 예제 한 개만 학습에 사용할 경우), 이를 **확률적 경사 하강법(Stochastic Gradient Descent, SGD)**라고 한다.
    - 미니배치 기반 SGD를 사용하면 알고리즘을 병렬로 학습할때 계산량을 크게 줄일 수 있다. 그러나 모델을 충분히 훈련시키려면 반복 횟수를 늘려야 한다. 미니배치 기반 SGD는 수렴하기 더 어렵지만 최저점에 충분히 가깝게 갈 수 있다. 미니배치 데이터셋의 크기(k)를 작게 설정하면 알고리즘은 더 **확률적으로** 움직인다. 즉, 알고리즘이 비용 함수의 최저점을 향해 **더 무작위로** 움직인다는 의미다. 반면 k값으 크게 설정하면 알고리즘은 더욱 안정적으로 이동한다. 하지만 어느 경우에든 최저점에 충분히 도달하며 BGD와 유사한 결과를 얻을 수 있다.
    - 스파크에서 사용할 경우 miniBatchFraction에 0~1값을 설정한다. 1로 설정하면 BGD와 마찬가지로 각 학습 단계에서 전체 데이터셋을 사용한다. 
    → 반복 회수 매개변수는 전체 데이터셋이 총 100회 정도 반복 사용할 수 있는 값을 대체로 선택한다.

        ```python
        def iterateLRwSGDBatch(iterNums, stepSizes, fractions, train, valid):
          for numIter in iterNums:
            for step in stepSizes:
              for miniBFraction in fractions:
                alg = LinearRegressionWithSGD()
                model = alg.train(train, intercept=True, iterations=numIter, step=step, miniBatchFraction=miniBFraction)
                rescaledPredicts = train.map(lambda x: (model.predict(x.features), x.label))
                validPredicts = valid.map(lambda x: (model.predict(x.features), x.label))
                meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
                meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
                print("%d, %5.3f %5.3f -> %.4f, %.4f" % (numIter, step, miniBFraction, meanSquared, meanSquaredValid))

        iterateLRwSGDBatch([400, 1000], [0.05, 0.09, 0.1, 0.15, 0.2, 0.3, 0.35, 0.4, 0.5, 1], [0.01, 0.1], trainHPScaled, validHPScaled)
        iterateLRwSGDBatch([400, 1000, 2000, 3000, 5000, 10000], [0.4], [0.1, 0.2, 0.4, 0.5, 0.6, 0.8], trainHPScaled, validHPScaled)
        ```

        결론적으로 미니배치 기반 SGD는 BGD와 비슷한 수준이 RSME를 달성할 수 있다. 그렇다면 계산 성능을 개선할 수 있는 미니배치 기반 SGD를 사용하는 편이 낫다. 

2. LBFGS 최적화: LBFGS(Limited-memory BFGS)는 제한된 메모리로 BFGS(Broyden-Fletcher-Goldfarb-Shanno)알고리즘을 추정하는 기법이다. BFGS는 다차원 함수를 최적화하는 알고리즘으로 어떤 함수의 이계도 함수를 표현한 행렬(헤세 행렬, Hessian matrix)를 구하고, 이 헤세 행렬의 역행렬을 근사치로 계산한 후 n X n 행렬을 메모리에 유지한다. 반면 LBFGS는 과거 갱신 값을 최근 열 개 미만으로 유지하므로 차원 개수가 많을 때 메모리를 더 효율적으로 사용한다. 

    → LBFGS는 지금까지 RMSE 결과와 거의 유사한 결과를 보이면서도 계산 성능을 크게 개선할 수 있다.
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 8</b> ML: classification and clustering</summary>
   <blockquote>
    <details close>
      <summary>8.1. Spark ML library </summary>
      
스파크에서 사용할 수 있는 분류 알고리즘에는 로지스틱회귀, 나이브 베이즈, 서포트 벡터 머신, 의사 결정 트리, 랜덤 포레스트가 있다. 스파크 ML의 주요 컴포넌트인 추정자, 변환자, 평가자, ML 매개변수, 파이프라인을 자세히 알아보자.
<img width="540" alt="Screen Shot 2021-06-28 at 10 54 00 PM" src="https://user-images.githubusercontent.com/43725183/123648225-bc73ce00-d863-11eb-888d-6b1238f7f30c.png">


1. **변환자(transformer)**: 한 데이터셋을 다른 데이터셋으로 변환하는 머신 러닝 컴포넌트를 구현할 수 있다. e.g. `transform`
2. **추정자(estimator)**: 주어진 데이터셋을 학습해 변환자(transformer)를 생성한다. e.g. `fit`
3. **평가자(evaluator)**: 모델 성능을 단일 지표로 평가한다. e.g. RMSE
4. ML 매개변수: `Param` 클래스로 매개변수 유형을 정의 → `ParamPair`에는 매개변수 유형(즉, `Param` 객체)과 변수 값의 쌍을 저장 → `ParamMap`은 여러 `ParamPair` 객체를 저장한다. 
(지정방법 1) ParamPair 또는 ParamMap 객체를 추정자의 `fit` 메서드 또는 변환자의 `transform` 메서드에 전달한다. 

    (지정방법 2) 각 매개변수의 set 메서드를 호출한다. e.g. `setRegParam(0.1)`

5. ML 파이프라인(`PipelineModel`): 앞의 객체들을 결합해 파이프라인을 구성할 수 있다.
    </details>
    <details close>
      <summary>8.2. Logistic regression </summary>
    </details>
    <details close>
      <summary>8.3. Decision trees and random forests </summary>
    </details>
    <details close>
      <summary>8.4. Using k-means clustering </summary>
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 9</b> connecting the dots with graphX</summary>
</details>  


## 3장 spark ops

<details close>
  <summary><b>chapter 10</b> Running spark</summary><br>
    <blockquote>
    <details close>
    <summary>10.1 An overview of Spark’s runtime architecture</summary>
      
* 스파크 런타임 컴포넌트: Spark는 실행되는 동안 다음 세 종류의 process를 생성한다. 아래 세 process는 Spark runtime architecture를 구성하는 **component(컴포넌트)** 들이다.<br>
    <img width="623" alt="Screen Shot 2021-06-21 at 6 41 04 PM" src="https://user-images.githubusercontent.com/43725183/122741775-43eb9b00-d2c0-11eb-958e-60b43ef70266.png">

    Driver의 위치는 클러스터 유형과 설정에 따라 다르다. 

    1. **client-process(클라이언트)**: driver process를 생성하는 프로세스로 spark 동작을 위한 classpath, configuration option 뿐만 아니라 application 변수를 포함한다. spark-submit script, spark-shell script 또는 custom application의 형태로 제공된다.  
    2. **driver(드라이버)**: Spark application에는 반드시 한 개의 driver process가 필요하다. driver process는 SparkContext와 Scheduler를 포함하며, 전체 spark application의 execution을 총괄하는 main function과 같은 존재이다.
        - driver process와 그 하위 컴포넌트(sparkcontext, scheduler)는 
        (1) cluster manager에게 memory와 CPU resource를 요청한다.
        (2) application logic을 stage와 task 단위로 조각낸다.
        (3) 각 task를 executor들에게 나눠준다.
        (4) 각 executor의 결과를 모은다.
        - deploy mode에 따른 분류 : driver process가 client JVM에 포함되면 **client-deploy mode**라고 부르고, client JVM과 분리되어 cluster내의 하나의 JVM으로 따로 구현되면 **cluster-deploy mode**라 부른다.
        - driver process가 시작될 때 특정 application의 configuration이 반영된 `SparkContext` instance가 생성된다. 스파크 REPL 셸은 셸 자체가 드라이버 프로그램 역할을 하며, 스파크 컨텍스트를 미리 설정해 `sc`라는 변수로 제공한다. JAR파일을 제출하거나 또 다른 프로그램에서 스파크 API로 스파크 독립형 애플리케이션을 실행할 때는 애플리케이션에서 직접 스파크 컨텍스트를 생성해야 한다.  JVM당 하나의 `SparkContext`가 필요하다(여러개의 SparkContext instance를 생성할 수도 있지만, 사용하지 말자). 
        → 스파크 컨텍스트는 RDD를 생성하거나 데이터를 로드하는 등 다양한 작업을 수행하는 여러 유용한 메서드를 제공하며, 스파크 런타임 인스턴스에 접근할 수 있는 기본 인터페이스이다.
    3. **executor(실행자)**: driver로부터 받은 task를 수행하고, 그 결과를 다시 driver로 보내는 역할을 한다. executor는 여러 개의 task slot을 가지고 있는데, 이는 thread로 구현되어 병렬 작업을 가능하게 한다. 일반적으로 CPU core 수의 2,3배 값을 task slot으로 설정한다. task slot은 스레드로 구현되므로 머신의 물리적인 CPU 코어 개수와 반드시 일치할 필요는 없다. 
- 스파크 클러스터 유형
    - Spark on clusters
        - **Standalone cluster**: 스파크 자체 클러스터. 스파크 자체 클러스터는 스파크 전용 클러스터다. 이 클러스터는 오직 스파크 애플리케이션에만 적합하도록 설계되었기 때문에 kerberos 인증 프로토콜로 보호된 HDFS를 지원하지 않는다. 이러한 보안 기능이 필요할 경우에는 YARN을 사용해 스파크를 실행해야 한다. 반면 스파크 자체 클러스터는 잡 시작에 걸리는 시간이 YARN보다 더 짧다.
        - **YARN cluster**: 하둡의 리소스 매니저 및 작업 실행 시스템이다. 하둡 버전 1의 맵리듀스 엔진을 대체한 것으로 맵리듀스 버전2라고도 한다.
        - **Mesos cluster**: 메소스는 확장성과 장애 내성을 갖춘 c++기반 분산 시스템 커널이다.
    - **Spark local modes**
    : special cases of a Spark "standalone cluster" running on "**a single machine**"
        - Spark local mode
        - Spark local cluster mode
    </details>
    <details close>
    <summary>10.2. Job and resource scheduling</summary>
      
* 스파크 애플리케이션의 리소스 스케줄링은 먼저 (1) 실행자(JVM 프로세스)와 (2) CPU(태스크 슬롯) 리소스를 스케줄링한 후, 각 실행자에 (3) 메모리 리소스를 할당하는 순서로 진행한다.
- 애플리케이션의 드라이버와 실행자가 시작되면 스파크 스케줄러는 이들과 직접 통신하면서 어떤 실행자가 어떤 태스크를 수행할지 결정한다. 스파크에서는 이 과정을 **잡 스케줄링(job scheduling)** 이라고 한다.
- 스파크 애플리케이션의 리소스 스케줄링은 다음 두 가지 레벨로 이루어진다.
    1. **클러스터 리소스 스케줄링**: 단일 클러스터에서 실행하는 다수의 애플리케이션에 클러스터의 리소스를 나누어주는 작업.
    2. **스파크 잡 스케줄링**: 클러스터 매니저가 CPU와 메모리 리소스를 각 실행자에 할당하면 스파크 애플리케이션 내부에서는 잡 스케줄링이 진행된다. 잡 스케줄링은 클러스터 매니저와 관계없이 스파크 자체에서 수행하는 작업으로, 잡을 어떻게 태스크로 분할하고 어떤 실행자에게 전달할지 결정한다. **스파크는 RDD 계보를 바탕으로 잡과 스테이지, 태스크를 생성한다.** **그 다음 스파크 스케줄러는 생성한 태스크를 실행자에 분배하고 실행 경과를 모니터링한다.** 
        - CPU 리소스 분배( = task 할당)
            - (1) **선입선출(FIFO) 스케줄러** :가장 먼저 리소스를 요청한 잡이 모든 실행자의 태스크 슬롯을 필요한 만큼 전부 차지한다. FIFO는 스파크의 기본 스케줄링 모드이며, 한 번에 잡 하나만 실행하는 단일 사용자 애플리케이션에 적합하다. → 아래 그림을 보면 spark job 1만 실행중이다. <br>
                <img width="527" alt="Screen Shot 2021-06-27 at 1 35 49 PM" src="https://user-images.githubusercontent.com/43725183/123532940-97d90280-d74c-11eb-8e82-874c6c49b049.png">

                
            - (2) **공정(FAIR) 스케줄러:** 라운드 로빈 방식으로 스파크 잡들에 균등하게 리소스를 분배한다.→ 아래 그림을 보면 spark job 1, 2 모두 실행중이다. <br>
                <img width="534" alt="Screen Shot 2021-06-27 at 1 36 14 PM" src="https://user-images.githubusercontent.com/43725183/123532953-a58e8800-d74c-11eb-93dc-0a54387b92be.png">

                
            - **태스크 예비 실행(speculative execution)**: 스파크의 예비 실행은 straggler 태스크(동일 스테이지의 다른 태스크보다 더 오래 걸리는 태스크)문제를 해결할 수 있다. 예를 들어 다른 프로세스가 일부 실행자 프로세스의 CPU 리소스를 모두 점유할 때 해당 실행자는 태스크를 제 시간에 완수하지 못할 수 있다. 이때 예비 실행 기능을 사용하면 스파크는 해당 파티션 데이터를 처리하는 동일한 태스크를 다른 실행자에도 요청한다. 기존 태스크가 지연되고 예비 태스크가 완료되면 스파크는 기존 태스크의 결과 대신 예비 태스크의 결과를 사용한다.

                하지만 일부 작업에서는 예비 실행하는 사용하는 것이 적절하지 않다. 예를 들어 관계형  데이터베이스아 같은 외부 시스템에 데이터를 내보내는 작업에 예비 실행을 적용하면 두 태스크가 동일 파티션의 동일 데이터를 외부 시스템에 중복 기록하는 문제가 발생할 수 있기 때문이다. 

            - **데이터 지역성(data locality):** 스파크가 데이터와 최대한 가까운 위치에서 태스크를 실행하려고 노력하는 것을 의미한다. 스파크는 각 파티션별로 선호 위치(preferred location) 목록을 유지한다. 파티션의 선호 위치는 파티션을 저장한 호스트네임 또는 실행자 목록으로, 이 위치를 참고해 데이터와 가까운 곳에서 연산을 실행할 수 있다. 스파크는 HDFS 데이터로 생성한 RDD와 캐시된 RDD에서 사용할 때만 선호 위치 정보를 알아낼 수 있다. → 스파크가 파티션의 선호 위치 목록을 확보하면 스파크 스케줄러는 파티션 데이터를 실제로 저장한 실행자가 관련 태스크를 실행하도록 최대한 노력하고, 데이터 전송을 최소화한다. 특정 태스크의 지역성 레벨은 스파크 웹 UI Stage Details 페이지에 있는 Tasks 테이블의 Locality Level 칼럼에 표시된다.
        - 메모리 리소스 스케줄링: 스파크 실행자 JVM 프로세스 메모리는 클러스터 매니저가 할당한다(cluster-deploy mode에서는 드라이버의 메모리도 할당한다.→ local mode도!) 각 프로세스에 메모리를 할당하면 스파크는 잡과 태스크가 사용할 메모리 리소스를 스케줄링하고 관리한다.
            - 클러스터 매니저가 관리하는 메모리: 클러스터 매니저는 `spark.executor.memory` 매개변수에 지정된 메모리 용량을 실행자에 할당한다. 변수 값 뒤에 g(기가 바이트)나, m(메가 바이트) 접미사를 붙여 단위를 지정할 수 있다. 스파크는 이 메모리를 나누어 사용한다.
            - 스파크가 관리하는 메모리(버전 1.5.2. 이하): 실행자에 할당된 메모리 중 일부를 나누어서 각각 **캐시데이터**와 임시 **셔플링 데이터**를 저장하는 데 사용한다.(→ *1.6.0 버전 이후로는 실행 메모리 영역과 스토리지 영역을 통합해 관리한다.*) 캐시 스토리지 크기는 `spark.storage.memoryFraction`(default = 0.6) 로 설정하며, 임시 셔플링 공간 크기는 `spark.shuffle.memoryFraction`(default = 0.2)로 설정한다. 하지만 스파크가 메모리 사용량을 측정하고 제한하기 전에 사용량이 설정 값을 초과로 할 수 있으므로 각각 `saprk.storage.safetyFraction`(default = 0.9)과 `spark.shuffle.safetyFraction`(default = 0.8)의 값이 추가로 필요하나다. 이 변수들은 셔플링과 캐시 스토리지의 공간을 지정된 비율만큼 낮춘다.

                → 즉, 캐시 데이터 스토리지의 실제 힙 메모리 비율은 `0.6*0.9 = 54%`<br>
                → 셔플링 데이터를 임시로 저장하는 힙 메모리 비율은 `0.2*0.8 = 16%`이다.

                남은 80%는 태스크를 실행하는데 필요한 기타 자바객체와 리소스를 저장하는 데 사용한다.

            - 드라이버 메모리 설정
                - 드라이버의 메모리 리소스는 `spark.driver.memory`로 설정한다. 이 변수는 spark-shell이나 spark-submit 스크립트로 애플리케이션을 시작할 때 적용된다(클러스터 배포 모드와 클라이언트 배포 모드에 모두 적용됨)
                - 반면 다른 외부 애플리케이션의 내부 코드에서 스파크 컨택스트를 동적으로 생성할 때는 드라이버가 외부 애플리케이션의 메모리 중 일부를 사용하므로 드라이버의 메모리 공간을 늘리려면 자바의 `-Xmx` 옵션을 사용해 애플리케이션 프로세스에 할당할 자바 힙의 최대 크기를 늘려야한다.
    </details>
    <details close>
    <summary>10.3. Configuring Spark</summary>
      
스파크 환경 매개변수를 설정하는 여러가지 방법(1~4)이 있지만 어떠한 방식으로 설정하든 애플리케이션에 적용한 모든 환경 매개변수 값은 `SparkConext`의 `SparkConf` 객체에 저장된다. spark-shell은 SparkContext를 자동으로 초기화해서 sc 벼수로 제공한다. SparkConf 객체는 다음과 같이 SparkContext의 getConf 메서드로 가져올 수 있다.(`sc.getConf()`) 

1. 스파크 환경 설정 파일: 스파크 환경 매개변수의 기본 값은 `SPARK_HOME/conf/spark-defeaults.conf` 파일에 지정한다. 만약 별도의 환경 설정 파일을 사용하려면 `--properties-file` 명령줄 매개변수로 파일 경로를 변경해야 한다. 
2. 명령줄 매개변수: 스파크는 환경 설정파일로 설정한 값보다 명령줄 매개변수로 지정한 값을 우선시한다. 명령줄 매개변수와 환경 설정 파일의 매개변수는 동일 변수라도 이름이 서로 다르다. 또 전체 스파크 환경 매개변수 중 일부만 명령줄 매개변수로 지정할 수 있다. (→ 명령줄 매개변수의 전체 목록은 spark-shell또는 spark-submi의 `—-help` 옵션을 사용해 확인할 수 있다)
      
    <img width="1000" alt="Screen Shot 2021-06-27 at 1 32 57 PM" src="https://user-images.githubusercontent.com/43725183/123532894-3153e480-d74c-11eb-86e2-7f99f90a799d.png">  
    
    ```bash
    spark-shell --driver-memory 16g
    spark-shell --conf spark.driver.memory=16g
    ```

    위의 두 줄은 동일하게 드라이버의 메모리를 16g로 설정하는 명령이다. `--conf`를 사용하면 명령줄에서도 환경 설정 파일과 동일한 이름으로 매개변수를 설정할 수 있다. 이 방식으로 환경 매개변수를 여러개 지정하려면 `--conf` 또한 매개변수마다 별도로 사용해야 한다. 

3. 시스템 환경 변수: 환경 매개변수 중 일부는 `SPARK_HOME/conf/[spark-env.sh](http://spark-env.sh)` 파일로 지정할 수 있다. 이 매개변수들은 기본 값은 OS 한경 변수를 사용해 설정할 수도 있다. 시스템 환경 변수로 설정한 방식은 모든 설정 방식 중에서 가장 낮은 우선순위로 적용된다. `spark-env.sh.template`을 참고하자. 
cf. 스파크 자체 클러스터의 spark-env.sh 파일을 변경했다면 모든 실행자가 동일한 환경 설정을 참고하도록 이 파일을 모든 워커 노드에 복사해야한다. 
4. 프로그램 코드로 환경 설정: 다음과 같이 SparkConf 클래스로 프로그램 내에서 직접 스파크 환경 매개변수를 설정할 수도 있다. 그러나 런타임중에서는 변경할 수 없으므로 반드시 SparkContext 객체를 생성하기 전에 SparkConf 객체의 설정을 마쳐야 한다. 

    ```python
    from pyspark.conf import SparkConf
    from pyspark.context import SparkContext
    conf = SparkConf()
    conf.set("spark.driver.memory", "16g")
    conf.get("spark.driver.memory")  #16g
    #conf.setAppName("<new name>")  #spark app 이름 변경
    sc = SparkContext(conf)
    ```

5. master 매개변수: 애플리케이션을 실행할 스파크 클러스터의 유형을 지정한다. 

    ```bash
    spark-submit --master <master_connection_url>
    ```

    ```python
    from pyspark.conf import SparkConf
    from pyspark.context import SparkContext
    conf = SparkConf()
    conf.set("spark.master", "<master_connection_url>")
    #conf.setMaster("<master_connection_url>")  #스파크 클러스터 유형 지정
    sc = SparkContext(conf)
    ```

6. 설정된 매개변수 조회

    ```python
    sc.getConf().getAll()
    ```
    <img width="1000" alt="Screen Shot 2021-06-27 at 1 33 30 PM" src="https://user-images.githubusercontent.com/43725183/123532903-46307800-d74c-11eb-8bed-3f8d5c52c39d.png">


    </details>
    <details close>
    <summary>10.4. Spark web UI</summary><br>
      
스파크는 SparkContext를 생성하면서 동시에 스파크 웹 UI를 시작한다. 

- **Jobs** 페이지: 현재 실행중인 잡, 완료된 잡, 실패한 잡의 통계 정보를 제공한다. 각 잡의 시작 시각과 실행시간, 실행 완료된 스테이지 및 태스크 정보를 확인할 수있다.
- **Stages** 페이지: 잡의 스테이지를 요약한 정보를 제공한다. 여기서 각 스테이지의 시작 시각, 실행 시간, 실행 현황, 입출력 데이터의 크기, 셔플링 읽기, 쓰기 데이터양을 확인할 수 있다.  + 누적변수도 확인가능!

    → 각 스테이지의 Description 칼럼을 클릭하면 해당 스테이지의 세부 페이지로 이동한다. 스테이지 세부 페이지에서는 잡 상태를 디버깅하는 데 유용한 정보를 얻을 수 있다. 예를 들어 잡의 실행 시간이 예상보다 오래 걸리면 스테이지 세부 페이지를 살펴보고 지연을 유발하는 스테이지와 태스크를 찾아서 문제 범위를 좁힐 수 있다. 

    - 예1: GC Time(실행자가 자바 가비지 컬렉션을 실행하려고 태스크를 잠시 중단한 시간)이 과다하다면 실행자에 메모리 시로를 더 많이 할당하거나 RDD의 파티션 개수를 늘려야한다.(파티션을 추가하면 파티션당 요소 개수가 감소해 메모리 사용량을 줄일 수 있다)
    - 예2: 셔플링 읽기, 쓰기 처리량이 과다하다면 프로그램 로직을 변경해 불필요한 셔플링을 피해야 한다
- **Storage** 페이지: 캐시된 RDD 정보와 캐시 데이터가 점유한 메모리, 디스크 타키온 스토리지 용량을 보여준다.
- **Environment** 페이지: 스파크 환경 매개변수뿐만 아니라 자바 및 스칼라의 버전, 자바 시스템 속성, 클래스패스 정보를 확인할 수 있다.
- **Executors** 페이지: 클러스터 내 모든 실행자 목록(드라이버 포함)과 각 실행자(또는 드라이버)별 메모리 사용량을 포함한 여러 통계 정보를 제공한다. Storage Memory에 표시된 숫자는 스토리지 메모리의 용량이다.(10.2.4절의 54%)

    각 실행자의 Thread Dump 링크를 클릭하면 해당 프로세스 내 모든 스레드의 현재 스택 추적 정보를 볼 수 있다. 스택 추적 정보는 대기 및 교착 상태(deadlock) 등으로 프로그램 실행이 느려지는 현상을 디버깅하는데 유용하다.
    </details>
    <details close>
    <summary>10.5. Running Spark on the local machine</summary>
      
* **로컬 모드**: 책의 대부분의 예제를 스파크 로컬 모드로 실행했다. 스파크 로컬 모드는 아직 대용량 클러스터에 접근할 권한이 없거나 간단한 아이디어 및 프로그램을 빠르게 테스트할 때 유용하다.

    로컬 모드에서는 클라이언트 JVM 내에 드라이버와 실행자를 각각 하나씩만 생성한다. 그러나 실행자는 스래드를 여러 개 생성해 태스크를 병렬로 실행할 수 있다. <br>
    <img width="612" alt="Screen Shot 2021-06-27 at 1 30 48 PM" src="https://user-images.githubusercontent.com/43725183/123532861-e508a480-d74b-11eb-8ec7-257dd77440f1.png">

    스파크 로컬 모드는 클라리언트 프로세스를 마치 클러스터의 단일 실행자처럼 사용하며, master 매개변수에 지정된 스레드 개수는 곧 병렬 태스크 개수를 의미한다. 따라서 머신의 CPU 코어 개수보다 더 많은 스레드를 지정하면 CPU 코어를 더 효율적으로 활용할 수 있다.(→ 코어 개수의 두세 배 정도로 e.g. 쿼드 코어라면 스레드는 8~12개. 최소한 2개는 하기)

    - `local[<n>]`: 스레드 <n> 개를 사용해 단일 실행자를 실행한다.
    - `local`: 스레드 하나. `local[1]` 과 동일하다.
    - `local[*]`: 스레드 개수를 로컬 머신에서 사용가능한 CPU 코어 개수와 동일하게 설정해 단일 실행자를 실행한다. 즉, 모든 CPU 코어를 전부 사용한다. → default
    - `local[<n>, <f>]`: 스레드를 <n>개 사용해 단일 실행자를 실행하고, 태스크당 실패를 최대 <f>번까지 허용한다. 이 모드는 주로 스파크 내부 테스트에 사용한다.
- **로컬 클러스터 모드:** 주로 스파크 내부 테스트 용으로 사용. IPC가 필요한 기능을 빠르게 테스트하거나 시연할 때도 유용하다. 로컬 클러스터 모드는 Spark standalone cluster를 로컬 머신에서 실행하는 것이다. 둘의 차이는 마스터를 별도 프로세스가 아닌 클라이언트 JVM에서 실행한다는 것이다. Spark standalone cluster에서 적용할 수 있는 환경 매개변수 대부분은 로컬 클러스터 모드에도 적용된다.`local-cluster[<n>, <c>, <m>]`: 로컬 머신에서 스레드 <c>개와 <m> MB 메모리를 사용하는 실행자를 <n>개 생성해 스파크 자체 클러스터를 실행하라는 의미이다. 
→ 로컬 클러스터 모드에서는 각 실행자를 별도의 JVM에서 실행하므로 스파크 자체 클러스터와 거의 유사하다.
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 11</b> Running on a spark standalone cluster</summary>
    <blockquote>
    <details close>
    <summary>11.1. Spark standalone cluster components</summary>
      
Standalone 클러스터는 master 프로세스와 worker(또는 slave) 프로레스로 구성된다. (아래 두 프로세스는 클러스터 관련 프로세스로 스파크 런타임 컴포넌트와는 별개의 개념이다)

- **master 프로세스**: 클러스터 매니저 역할을 한다. 즉, 클라이언트가 실행에 요청한 애플리케이션을 받고, 각 애플리케이션에 워커 리소스(CPU 코어)를 스케줄링한다.
- **worker 프로세스**: 애플리케이션의 태스크를 처리할 실행자를 시작한다.(클러스터 배포 모드에서는 애플리케이션의 드라이버를 실행하는 역할도 워커가 담당한다)

→ 앞서 말했듯이 드라이버는 스파크 잡을 조정하고 모니터링하는 컴포넌트이며, 실행자는 잡의 태스크를 실행하는 컴포넌트이다. 
→ 스파크 자체 클러스터를 구성하려면 클러스터의 모든 노드가 워커의 역할을 하도록 각 노드에 스파크를 설치해야한다. 

다음의 노드 두개로 구성된 스파크 standalone 클러스터에서는 마스터 프로세스 하나와 워커 프로세스 두개를 실행하며 다음과 같은 순서로 애플리케이션을 시작한다. (아래 예시는 cluster deploy mode)<br>
<img width="556" alt="Screen Shot 2021-06-28 at 10 55 09 PM" src="https://user-images.githubusercontent.com/43725183/123648415-e5945e80-d863-11eb-8bce-da792def25bd.png">

1. 클라이언트 프로세스는 애플리케이션을 마스터에 제출한다.
2. 마스터는 1번 노드의 워커에 드라이버를 시작하라고 지시한다. 
3. 1번 노드의 워커는 드라이버 JVM을 시작한다.
4. 마스터는 두 워커에 애플리케이션의 실행자를 시작하라고 지시한다. 
5. 두 워커는 각각 실행자 JVM을 시작한다.
6. 드라이버는 실행자와 직접 통신하며 스파크 애플리케이션을 실행한다. **클러스터의 프로세스들은 더 이상 관여하지 않는다.** 

→ 각 실행자는 다수의 스레드(CPU 코어)를 할당받는다. 이는 다수의 태스크를 병렬로 실행할 수 있는 태스크 슬롯을 의미한다. 
→ 스파크 버전 1.4.0부터는 워커당 실행자를 여러개 생성할 수 있다. 그러나 JVM 힙이 너무 크고(64GB 이상) 가비지 컬렉션이 잡 성능에 영향을 미친다면 워커 프로세스의 수를 늘려야할 수도 있다.

→ 클러스터는 여러 애플리케이션을 동시에 실행할 수 있으며, 각 애플리케이션에는 드라이버와 실행자들이 독립적으로 할당된다.
    </details>
    <details close>
    <summary>11.2. Starting the standalone cluster</summary>
    </details>
    <details close>
    <summary>11.3. Standalone cluster web UI</summary>
    </details>
    <details close>
    <summary>11.4. Running applications in a standalone cluster</summary>
    </details>
    <details close>
    <summary>11.5. Spark History Server and event logging</summary>
    </details>
    <details close>
    <summary>11.6. Running on Amazon EC2</summary>
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 12</b> Running on YARN and Mesos</summary><br>
</details>  

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
