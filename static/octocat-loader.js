// Load the GitHub-corner snippet into the page
document.addEventListener('DOMContentLoaded', () => {
    fetch('/static/octocat.html')
      .then(res => res.text())
      .then(html => {
        // insert at very top of body
        document.body.insertAdjacentHTML('afterbegin', html);
      })
      .catch(console.error);
  });
  