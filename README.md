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
  </details>
  <details close>
  <summary>2.2. Using Spark shell and writing your first Spark program</summary>
  </details>
  <details close>
  <summary>2.3. Basic RDD actions and transformations</summary>
  </details>
  <details close>
  <summary>2.4. Double RDD functions</summary>
  </details></blockquote>
</details>


## 2장 meet the spark family

## 3장 spark ops

## 4장 Bringing it together

### Appendix
<details close>
<summary>A. Installing Apache Spark</summary>
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
