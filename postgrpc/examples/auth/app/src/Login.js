import { ClipLoader } from 'react-spinners'
import { Link } from 'react-router-dom'
import { toFields, useLoginFlow } from './auth'

const Login = () => {
  const [flow, completeFlow] = useLoginFlow()

  const onSubmit = async event => {
    event.preventDefault()
    completeFlow(new FormData(event.target))
  }

  return flow
    ? <>
      <h1>Login</h1>
      <form onSubmit={onSubmit}>
        {toFields(flow)}
      </form>
      <hr />
      <Link to='/auth/registration'>Sign up</Link>
    </>
    : <ClipLoader color='white' />
}


export default Login
