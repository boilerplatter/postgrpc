import 'react-toastify/dist/ReactToastify.css'
import './App.css'
import { ToastContainer } from 'react-toastify'
import { BrowserRouter as Router, Route } from 'react-router-dom'
import Dashboard from './Dashboard'
import Login from './Login'
import Registration from './Registration'

const App = () =>
  <div className='App'>
    <Router>
      <Route path='/auth/login' exact>
        <Login />
      </Route>
      <Route path='/auth/registration' exact>
        <Registration />
      </Route>
      <Route path='/' exact>
        <Dashboard />
      </Route>
    </Router>
    <ToastContainer limit={2} position='bottom-center' theme='colored' />
  </div>

export default App
