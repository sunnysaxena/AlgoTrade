function showTable(data, divShowData){
    var myHolding = data

    var col = [];
    for(var i = 0; i < myHolding.length; i++){
        for (var key in myHolding[i]){
            if (col.indexOf(key) === -1){
                col.push(key);
            }
        }
    }

    // create a table
    var table = document.createElement("table");

    // Create table header row using the extracted headers above.
    var tr = table.insertRow(-1);                   // table row.

    for (var i = 0; i < col.length; i++) {
      var th = document.createElement("th");      // table header.
      th.innerHTML = col[i];
      tr.appendChild(th);
    }

    // add json data to the table as rows.
    for (var i = 0; i < myHolding.length; i++) {

      tr = table.insertRow(-1);

      for (var j = 0; j < col.length; j++) {
        var tabCell = tr.insertCell(-1);
        tabCell.innerHTML = myHolding[i][col[j]];
      }
    }

    // Now, add the newly created table with json data, to a container.
    // var divShowData = document.getElementById('showData');
    divShowData.innerHTML = "";
    divShowData.appendChild(table);
    divShowData.style.display = "block";

}

function getHolding(){
    var divShowData = document.getElementById("showHolding");
    if(hideShowDiv(divShowData))
        return
    var url = "http://127.0.0.1:5000/get_holdings";
    $.get(url, function(data, status){
        console.log(data)
        showTable(data.data.holding, divShowData)
    });
}

function getProfile(){
    var profileinfo = document.getElementById("showProfile");
    if(hideShowDiv(profileinfo))
        return
    var url = "http://127.0.0.1:5000/get_profile";
    $.post(url, function(data, status){
        console.log(data.data)
        profileinfo.innerHTML = data.data.result.name;
        profileinfo.style.display = "block";
        console.log(status);
    });
}

function getFund(){
    var divShowData = document.getElementById("fundinfo");
    if(hideShowDiv(divShowData))
        return
    var url = "http://127.0.0.1:5000/get_funds";
    $.post(url, function(data, status){
        console.log(data.data.fund_limit);
        showTable(data.data.fund_limit,divShowData)
    });
}

function hideShowDiv(divHide){
    if(divHide.style.display === "block"){
        divHide.style.display = "none";
    }
}