<template>
  <div class="container">
    <nav class="navbar navbar-dark bg-white">
    <a href="#" class="navbar-brand">
        <img src="../assets/logo.svg" height="38" alt="CoolBrand">
    </a>
</nav>
    <div class="row">
      <div class="col-sm-10">
        <div class="header">
          <h1>Fetch Daily Stock Prices</h1>
        </div>
        <br><br>
        <div class="bar">
          <div class="hsuHs">
            <span class="wFncld z1asCe MZy1Rb">
              <svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M15.5 14h-.79l-.28-.27A6.471 6.471 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"></path></svg>
          </span>
            </div>
          <input class="searchbar" id="searchBar" list="stockNames" placeholder="Enter a stockname" type="text" title="Search">
        </div>

        <button v-on:click="search()" class="button">Search</button>

        <div v-if="displayLoader" class="spinner"> 

        </div>
        <datalist id="stockNames">
          <template v-for="(name , index) in stockNames" >
            <option v-bind:key="index" v-bind:value="name"/>
          </template>
        </datalist>
         <br><br>
        <table v-if="stockData.length > 0" class="table table-hover">
          <thead>
            <tr>
              <th scope="col">Code</th>
              <th scope="col">Name</th>
              <th scope="col">Open</th>
              <th scope="col">High</th>
              <th scope="col">Low</th>
              <th scope="col">Close</th>
              <th scope="col">Date</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="stock in stockData" :key="stock.SC_DATE">
            <td>{{ stock.SC_CODE }}</td>
            <td>{{ selectedStock }}</td>
            <td>{{stock.OPEN}}</td>
            <td>{{stock.HIGH}}</td>
            <td>{{stock.LOW}}</td>
            <td>{{stock.CLOSE}}</td>
            <td>{{stock.SC_DATE}}</td>            
            </tr>
          </tbody>
        </table>
      </div>
    </div>
    <div class="pageNav">
     <button v-if="previous !== ''" v-on:click="previousData()" type="button" class="navigation">Previous</button>

     <button v-if="next !== ''"   v-on:click="nextData()" type="button" class="navigation right">Next</button>
     
    </div><br><br>
    <button v-if="stockData.length > 0" id="csv" class="buttonCSV" v-on:click="exportCSV()">Download CSV</button>
    
  </div>
   
</template>

<style scoped>


@keyframes spinner {
  to {transform: rotate(360deg);}
}
 
.spinner:before {
  content: '';
  box-sizing: border-box;
  position: absolute;
  top: 100%;
  left: 46%;
  width: 20px;
  height: 20px;
  margin-top: -10px;
  margin-left: -10px;
  border-radius: 50%;
  border: 2px solid transparent;
  border-top-color: #07d;
  border-bottom-color: #07d;
  animation: spinner .8s ease infinite;
}
.buttonCSV{
  background-color: #00c140;
  border: none;
  color: white;
  padding: 4px 16px;
  text-align: center;
  text-decoration: none;
  display: inline-block;
  font-size: 15px;
  cursor: pointer;
  margin: 0 auto;
  display: block;
  margin-bottom:1vh;
}
.buttonCSV:hover{
  background-color: #02e24d;
}
.navigation{
   
  background-color: #0059c1;
  border: none;
  color: white;
  padding: 4px 16px;
  text-align: center;
  text-decoration: none;
  display: inline-block;
  font-size: 15px;
  margin: 4px 2px;
  cursor: pointer;
  margin-top: 1vw;
}

.right{
  float: right;
  margin-right: 11vw;
}

 .button {
  background-color: #0850a1;
  border: none;
  color: white;
  padding: 9px 36px;
  text-align: center;
  text-decoration: none;
  display: inline-block;
  font-size: 20px;
  margin: 4px 2px;
  cursor: pointer;
  border-radius: 2vw;
  margin-left: 21vw;
margin-top: 1vw;
}

.button:hover{
  background-color: #0059c1;
}
.hsuHs {
    display: contents;
    margin: auto;
}
.wFncld {
    margin-top: 3px;
    color: #9aa0a6;
    height: 20px;
    width: 20px;
}
.z1asCe {
    display: inline-block;
    fill: currentColor;
    height: 24px;
    line-height: 24px;
    position: relative;
    width: 24px;
    margin-left: 1vw;
    padding-top: 7px;
}
.z1asCe svg {
    display: block;
}
.header {
  padding: 10px;
  text-align: center;
  background: #0059c1;
  color: white;
  font-size: 1.75rem;
}
.row{
  margin-top: 2vw;
}
.bar{
  margin:0 auto;
  width:575px;
  border-radius:30px;
  border:1px solid #dcdcdc;
}
.bar:hover{
  box-shadow: 1px 1px 8px 1px #dcdcdc;
}
.bar:focus-within{
  box-shadow: 1px 1px 8px 1px #dcdcdc;
  outline:none;
}
.searchbar{
  height: 45px;
  border: 1px;
  width: 500px;
  font-size: 16px;
  outline: none;
  border-radius: 30px;
  padding-bottom: 1vw;
  padding-left: 1vw;
  padding-top: 1vw;
}
</style>

<script>
import axios from "axios"
export default {
  methods:{
    search: function(){
      console.log("Clicked!")
      let tickerName = document.getElementById('searchBar').value.toString()
      if (tickerName.length === 0 || tickerName === null || (this.selectedStock === tickerName)){
        return;
      }
      this.selectedStock = tickerName
      this.stockData = []
      this.displayLoader = true
      
      setTimeout(()=>{
         this.displayLoader = false
         axios.get('/api/stockData?ticker_name='+tickerName + '&start=1').then(
          response => {
        
         response.data.results.forEach(element => {
          element = JSON.parse(element)
          this.stockData.push(element)
        });
        
        this.next = response.data.next
        this.previous = response.data.previous
      }
    ).catch(
      err =>{
        console.log(err)
      }
    )
      } , 1000)
     
    },

    nextData: function(){
      if (this.next === ''){
        return 
      }
      
      this.stockData = []
      let startLimitArr = this.next.split(/[ .:;?!~,`"&|()<>{}\[\]\r\n/\\]+/).splice(-2)
       
      let start = startLimitArr[0].split('=')[1]
      this.displayLoader = true
      
      setTimeout(()=>{
       this.displayLoader = false   
      axios.get('/api/stockData?ticker_name='+this.selectedStock + '&start=' + start).then(
        response =>{
          response.data.results.forEach(element =>{
            element = JSON.parse(element)
            this.stockData.push(element)
          })
          this.next = response.data.next
          this.previous = response.data.previous
        }
      ).catch(err =>{
        console.log(err)
      })
      } , 1000)

    },
    previousData: function(){
      if (this.previous === ''){
        return 
      }
      
      this.stockData = []
      let startLimitArr = this.previous.split(/[ .:;?!~,`"&|()<>{}\[\]\r\n/\\]+/).splice(-2)
      let start = startLimitArr[0].split('=')[1]
      console.log(start)
      this.displayLoader = true
      
      setTimeout(()=>{
       this.displayLoader = false  
      axios.get('/api/stockData?ticker_name='+this.selectedStock + '&start=' + start).then(
        response =>{
          response.data.results.forEach(element =>{
            element = JSON.parse(element)
            this.stockData.push(element)
          })
          this.next = response.data.next
          this.previous = response.data.previous
        }
      ).catch(err =>{
        console.log(err)
      })
      } , 1000)

    },
    exportCSV : function(){
      axios.get('/api/csvData?ticker_name='+this.selectedStock).then(
        response =>{
          let jsonObj = [];
          response.data.results.forEach(element =>{
            element = JSON.parse(element)
            element['SC_NAME'] = this.selectedStock
            jsonObj.push(element)
          })
         let fields = Object.keys(jsonObj[0])
         var replacer = function(key, value) { return value === null ? '' : value } 
         var csv = jsonObj.map(function(row){
           return fields.map(function(fieldName){
             return JSON.stringify(row[fieldName] , replacer)
           }).join(',')
         })
           
          let link = document.createElement("a");

          csv.unshift(fields.join(',')) // add header column
          csv = csv.join('\r\n');
          let blob = new Blob([csv] , { type: 'text/csv;charset=utf-8;' })
          console.log(blob)
          let url = URL.createObjectURL(blob)
          link.setAttribute("href", url);
          link.setAttribute("download", 'filename.csv');
          link.style.visibility = 'hidden';  
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);        
           
        }
      )
    }

  },
  data(){
    return {stockData:[] , stockNames:[] , selectedStock:'' , next:'' , previous:'' , displayLoader:false}
  }
  ,
  mounted(){
    axios.get('/api/stockKeys').then(
      response => {
        
        this.stockNames = response.data
        console.log(this.stockNames)
      }
    ).catch(err => {
      console.log(err)
    })
    
  }
}
</script>