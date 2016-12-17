/* CHART 1 */
var canvas = document.getElementById('myChart1');
        var data = {
            labels: ["02116", "02119", "02120", "02122", "02124", "02127", "02128", "02130", "02132", "02135"],
            datasets: [
                {
                    label: "Number of Crimes",
                    backgroundColor: "rgba(255,99,132,0.2)",
                    borderColor: "rgba(255,99,132,1)",
                    borderWidth: 1,
                    hoverBackgroundColor: "rgba(255,99,132,0.4)",
                    hoverBorderColor: "rgba(255,99,132,1)",
                    data: [124, 178, 109, 118, 81, 92, 61, 58, 45, 72],
                }
            ]
        };

        var myBarChart = new Chart.Bar(canvas, {
            data: data,
            options: {
		        scales: {
		            yAxes: [{
		                ticks: {
		                    max: 180,
		                    min: 0,
		                    stepSize: 10
		                }
		            }]
		        }
	    	}
        }); 



/* CHART 2 */
var canvas = document.getElementById('myChart2');
        var data = {
            labels: ["02116", "02119", "02120", "02122", "02124", "02127", "02128", "02130", "02132", "02135"],
            datasets: [
                {
                    label: "Number of Public Schools",
                    backgroundColor: "rgb(191, 63, 82)",
                    borderColor: "rgba(255,99,132,1)",
                    borderWidth: 1,
                    hoverBackgroundColor: "rgba(255,99,132,0.4)",
                    hoverBorderColor: "rgba(255,99,132,1)",
                    data: [4, 13, 3, 5, 11, 8, 10, 11, 7, 7],
                }
            ]
        };

        var myBarChart = new Chart.Bar(canvas, {
            data: data,
            options: {
                scales: {
                    yAxes: [{
                        ticks: {
                            max: 14,
                            min: 0,
                            stepSize: 2
                        }
                    }]
                }
            }
        }); 

/* CHART 3 */
var canvas = document.getElementById('myChart3');
        var data = {
            labels: ["02119", "02120", "02121", "02122", "02124", "02125", "02126", "02127", "02128", "02131", "02132", "02135", "02136", "02148", "02189"],
            datasets: [
                {
                    label: "Average Earnings of Police Officers",
                    backgroundColor: "rgb(55, 58, 150)",
                    borderColor: "rgba(55,58,150,0.6)",
                    borderWidth: 1,
                    hoverBackgroundColor: "rgba(255,99,132,0.4)",
                    hoverBorderColor: "rgba(255,99,132,1)",
                    data: [88222.93, 2369.58, 169968.38, 138971.46, 135889.69, 95076.47, 121298.71, 108223.90, 
                    85686.92, 96050.91, 103801.04, 80056.36, 140107.19, 192075.99, 207311.15],
                }
            ]
        };

        var myBarChart = new Chart.Bar(canvas, {
            data: data,
            options: {
                scales: {
                    yAxes: [{
                        ticks: {
                            max: 190000,
                            min: 0,
                            stepSize: 20000
                        }
                    }]
                }
            }
        }); 


/* CHART 4 */
var canvas = document.getElementById('myChart4');
        var data = {
            labels: ["02116", "02119", "02120", "02122", "02124", "02127"],
            datasets: [
                {
                    label: "Number of Street Lights",
                    backgroundColor: "purple",
                    borderColor: "rgba(255,99,132,1)",
                    borderWidth: 1,
                    hoverBackgroundColor: "rgba(255,99,132,0.4)",
                    hoverBorderColor: "rgba(255,99,132,1)",
                    data: [400, 235, 346, 111, 120, 59],
                }
            ]
        };

        var myBarChart = new Chart.Bar(canvas, {
            data: data,
            options: {
                scales: {
                    yAxes: [{
                        ticks: {
                            max: 500,
                            min: 0,
                            stepSize: 50
                        }
                    }]
                }
            }
        });


        
var canvas = document.getElementById('myChart5');
     var scatterChart = new Chart.Scatter(canvas, {
        type: 'line',
        data: {
            datasets: [{
                label: 'Scatter Dataset',
                pointBackgroundColor: "rgb(0, 0, 0)",
                data: [{
                    x: 4,
                    y: 124
                }, {
                    x: 13,
                    y: 178
                }, {
                    x: 3,
                    y: 109
                }, {
                    x: 5,
                    y: 118
                }, {
                    x: 11,
                    y: 81
                }, {
                    x: 8,
                    y: 92
                }, {
                    x: 10,
                    y: 61
                }, {
                    x: 11,
                    y: 58
                }, {
                    x: 7,
                    y: 45
                }, {
                    x: 7,
                    y: 72
                }, {
                    x: 9,
                    y: 62
                }]
            }]
        },
        options: {
            showLines: false,
            scales: {
                xAxes: [{
                    type: 'linear',
                    position: 'bottom'
                }]
            }
        }
    });

