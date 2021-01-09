const numberInput = document.getElementById('number'),
      textInput = document.getElementById('msg'),
      button = document.getElementById('but'),
      response = document.querySelector('.response');

      button.addEventListener('click',send, false);

      function send(){
         

          fetch('/text', {
              method: 'post',
              headers: {
                  'Content-type':'application/json'
              },
              body: JSON.stringify({number: 263713123711, text: 'hello world'})
          })
          .then(function(res){
              console.log(res);
          })
          .catch(function(err){
              console.log(err);
          });
      }