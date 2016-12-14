const button = document.getElementsByClassName('button')[0];
const score = document.getElementsByClassName('score')[0];

fetch('/score')
.then(res => res.json())
.then(docs => {
  console.log(docs);
});
function submit() {
  window.requestAnimationFrame(() => button.classList += ' active');
  button.onClick = () => {};
  const data = {
    name: document.getElementById('name').value,
    latitude: document.getElementById('latitude').value,
    longitude: document.getElementById('longitude').value,
  }
  fetch('/score', {
    method: 'post',
    body: JSON.stringify(data),
    headers: {
      'Content-Type': 'application/json'
    }
  }).then(res => res.json()).then(res => {
    button.classList = 'button';
    button.onClick = submit;
    console.log(res)
    score.innerHTML = '<p class="score__label">Score:</p><p>' + res.score + '</p>';
    
  });
}
